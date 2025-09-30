// server.js
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const puppeteer = require('puppeteer');
const axios = require('axios');
const cheerio = require('cheerio');
const { sequelize, News, HistoricalData } = require('./models');
const Redis = require('ioredis');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const crypto = require('crypto');
const NodeCache = require('node-cache');
const { XMLParser } = require('fast-xml-parser');

const app = express();
const PORT = process.env.PORT || 3000;

// ============================ Firebase Admin (FCM) ============================
const admin = require('firebase-admin');

function getServiceAccountFromEnv() {
  const b64 = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64;
  if (!b64) throw new Error('Missing FIREBASE_SERVICE_ACCOUNT_BASE64');
  return JSON.parse(Buffer.from(b64, 'base64').toString('utf8'));
}
if (!admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(getServiceAccountFromEnv()) });
}
const fcm = admin.messaging();

/** ========================== Puppeteer Singleton =========================== */
let _browserPromise = null;

async function getBrowser() {
  if (_browserPromise) return _browserPromise;
  _browserPromise = puppeteer.launch({
    headless: true,
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--no-zygote',
      '--disable-gpu',
      '--single-process',
      '--disable-blink-features=AutomationControlled'
    ]
  });
  return _browserPromise;
}

async function withPage(run) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  try {
    return await run(page);
  } finally {
    try { await page.close(); } catch { }
  }
}

async function closeBrowser() {
  try { const b = await _browserPromise; await b?.close(); } catch { }
}

/** ===================== TOPIC MAP & PUSH (per-BAHASA) ===================== */

// peta kategori -> topic dasar (tanpa suffix bahasa)
function topicBaseFor(category = '') {
  const c = (category || '').toLowerCase();
  if (c.includes('commodity')) return 'news_commodity';
  if (c.includes('currenc')) return 'news_currencies';
  if (c.includes('index')) return 'news_index';
  if (c.includes('crypto')) return 'news_crypto';
  if (c.includes('economy') || c.includes('fiscal')) return 'news_economy';
  if (c.includes('analysis')) return 'news_analysis';
  return 'news_all';
}
function resolveLang(lang) {
  const l = String(lang || '').toLowerCase();
  return l === 'en' ? 'en' : 'id';
}

// dedupe push dgn redis (3 hari)
async function alreadyPushed(redis, key) {
  const k = `push:sent:${key}`;
  const ok = await redis.set(k, '1', 'NX', 'EX', 60 * 60 * 24 * 3);
  return ok === null; // true artinya SUDAH ada
}

// ⛳️ kirim ke 2 topic: (1) per-bahasa (news_id/news_en) — WAJIB untuk app kamu
//                        (2) per-kategori+bahasa (news_crypto_id) — OPSIONAL
async function pushNews({ id, title, summary, image, category, language = 'id' }) {
  const lang = resolveLang(language);
  const baseTopic = topicBaseFor(category);
  const topicLangOnly = `news_${lang}`;
  const topicCatLang = `${baseTopic}_${lang}`;

  const deeplink = `newsmaker23://news?id=${id}`;
  const collapseKey = `news_${id}_${lang}`;

  const notifBody =
    String(summary || (lang === 'id' ? 'Ketuk untuk membaca' : 'Tap to read more')).slice(0, 230);

  const commonMsg = {
    notification: {
      title: String(title || '').slice(0, 110),
      body: notifBody,
      image: image || undefined,
    },
    data: {
      news_id: String(id),
      url: deeplink,
      category: String(category || ''),
      language: lang,
      click_action: 'FLUTTER_NOTIFICATION_CLICK',
    },
    android: {
      notification: {
        channelId: 'high_importance_channel',
        imageUrl: image || undefined,
        priority: 'HIGH',
        defaultSound: true
      },
      collapseKey,
    },
    apns: { payload: { aps: { sound: 'default' } }, fcmOptions: { imageUrl: image || undefined } },
  };

  const resLang = await fcm.send({ topic: topicLangOnly, ...commonMsg });
  console.log('📣 Push sent:', resLang, '→', topicLangOnly);

  if (process.env.FCM_TOPIC_CATEGORY_LANG === '1') {
    const resCatLang = await fcm.send({ topic: topicCatLang, ...commonMsg });
    console.log('📣 Push sent:', resCatLang, '→', topicCatLang);
  }

  if (process.env.FCM_COMPAT_GLOBAL_TOPIC === '1') {
    const legacy = await fcm.send({ topic: baseTopic, ...commonMsg });
    console.log('📣 Push legacy:', legacy, '→', baseTopic);
  }
}

// ================================ hardening ================================
app.set('trust proxy', true);
app.use(helmet());
app.use(cors());
app.use(compression());
app.use(express.json());

// rate limit (express-rate-limit v7)
const limiter = rateLimit({
  windowMs: 60_000,
  limit: 120,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => req.ip,
  validate: { xForwardedForHeader: false }
});
app.use(limiter);

// global safety nets
process.on('unhandledRejection', (reason) => console.error('🧯 Unhandled Rejection:', reason));
process.on('uncaughtException', (err) => console.error('🧯 Uncaught Exception:', err));

// ================================= caching ================================
let cachedNews = [];
let cachedNewsID = [];
let cachedCalendar = [];
let cachedQuotes = [];
let lastUpdatedNews = null;
let lastUpdatedNewsID = null;
let lastUpdatedCalendar = null;
let lastUpdatedQuotes = null;

// Redis
const redis = new Redis(process.env.REDIS_PUBLIC_URL || {
  host: '127.0.0.1',
  port: 6379,
  retryStrategy: (times) => Math.min(times * 50, 2000),
});
redis.on('connect', () => console.log('✅ Redis connected'));
redis.on('error', (err) => console.error('❌ Redis error:', err));

// ================================== DB ====================================
(async () => {
  try {
    await sequelize.authenticate();
    await sequelize.sync();
    console.log('✅ MySQL connected & synced!');
  } catch (err) {
    console.error('❌ MySQL error:', err.message);
  }

  try {
    await redis.set('health:redis', 'ok', 'EX', 30);
    const pong = await redis.get('health:redis');
    console.log('🔁 Redis check:', pong);
  } catch (err) {
    console.error('❌ Redis check error:', err.message);
  }
})();

// ================================ helpers =================================
const MONTHS_EN = { jan: 0, january: 0, feb: 1, february: 1, mar: 2, march: 2, apr: 3, april: 3, may: 4, jun: 5, june: 5, jul: 6, july: 6, aug: 7, august: 7, sep: 8, sept: 8, september: 8, oct: 9, october: 9, nov: 10, november: 10, dec: 11, december: 11 };
const MONTHS_ID = { jan: 0, januari: 0, feb: 1, februari: 1, mar: 2, maret: 2, apr: 3, april: 3, mei: 4, jun: 5, juni: 5, jul: 6, juli: 6, agu: 7, agustus: 7, agst: 7, sep: 8, september: 8, okt: 9, oktober: 9, nov: 10, november: 10, des: 11, desember: 11 };

function parsePublishedAt(dateStr = '', lang = 'en') {
  const s = String(dateStr).trim().replace(/\s+/g, ' ');
  if (!s) return null;
  const m = s.match(/^(\d{1,2})\s+([A-Za-z\.]+)\s+(\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
  if (!m) return null;
  const day = parseInt(m[1], 10);
  const monRaw = m[2].toLowerCase().replace(/\./g, '');
  const year = parseInt(m[3], 10);
  const hourLocal = m[4] ? parseInt(m[4], 10) : 12;
  const minuteLocal = m[5] ? parseInt(m[5], 10) : 0;

  const map = (lang || '').toLowerCase() === 'id' ? MONTHS_ID : MONTHS_EN;
  const altMap = map === MONTHS_ID ? MONTHS_EN : MONTHS_ID;
  let month = map[monRaw];
  if (month == null) month = altMap[monRaw];
  if (month == null || !Number.isFinite(day) || !Number.isFinite(year)) return null;

  const WIB_OFFSET = 7;
  const dtUtc = new Date(Date.UTC(year, month, day, hourLocal - WIB_OFFSET, minuteLocal, 0));
  return Number.isNaN(+dtUtc) ? null : dtUtc;
}

function ensureDate(d) {
  const x = d instanceof Date ? d : new Date(d);
  return Number.isNaN(+x) ? new Date() : x;
}
function normalizeSpace(s) { return (s || '').replace(/\s+/g, ' ').trim(); }

const HTML_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
  'Cache-Control': 'no-cache',
  'Pragma': 'no-cache',
  'Connection': 'keep-alive',
  'Upgrade-Insecure-Requests': '1',
  'DNT': '1',
};
function makeHtmlHeaders(lang = 'en') {
  const isID = (lang || '').toLowerCase() === 'id';
  return { ...HTML_HEADERS, 'Accept-Language': isID ? 'id-ID,id;q=1,en;q=0.5' : 'en-US,en;q=1,id;q=0.5' };
}
function isWafOrChallenge($) {
  const t = $.text().toLowerCase();
  const ttl = $('title').text().toLowerCase();
  return (
    t.includes('access denied') ||
    t.includes('forbidden') ||
    t.includes('request blocked') ||
    t.includes('captcha') ||
    ttl.includes('forbidden') ||
    ttl.includes('blocked')
  );
}
const delay = (ms) => new Promise((r) => setTimeout(r, ms));
async function retryRequest(fn, retries = 3, delayMs = 500) {
  try { return await fn(); }
  catch (err) {
    if (retries === 0) throw err;
    await delay(delayMs);
    return retryRequest(fn, retries - 1, delayMs * 2);
  }
}
function sendWithETag(req, res, payload, maxAgeSec = 30) {
  const body = JSON.stringify(payload);
  const etag = crypto.createHash('md5').update(body).digest('hex');
  res.set('ETag', etag);
  res.set('Cache-Control', `public, max-age=${maxAgeSec}, stale-while-revalidate=60`);
  if (req.headers['if-none-match'] === etag) return res.status(304).end();
  return res.json(payload);
}
function makeNewsCacheKey({ lang, category, search, page, limit, fields }) {
  const f = (fields || '').split(',').map(s => s.trim()).sort().join('|');
  const c = category || 'all';
  const s = (search || '').trim();
  const qhash = crypto.createHash('md5').update(s).digest('hex');
  return `news:list:${lang}:cat:${c}:q:${qhash}:p:${page}:l:${limit}:f:${f}`;
}
const NEWS_ALLOWED_FIELDS = new Set(['id', 'title', 'link', 'image', 'category', 'date', 'summary', 'detail', 'language', 'createdAt', 'source_name', 'source_url', 'author', 'author_name', 'published_at']);
function normalizeFields(fields) {
  if (!fields) return null;
  const arr = fields.split(',').map(s => s.trim()).filter(Boolean);
  const picked = arr.filter(f => NEWS_ALLOWED_FIELDS.has(f));
  return picked.length ? picked : Array.from(NEWS_ALLOWED_FIELDS);
}

// ---- text sanitizers ----
function decodeUnicodeEscapes(s) {
  return String(s || '')
    .replace(/\u003E/g, '>')
    .replace(/\u003C/g, '<')
    .replace(/\u0026/g, '&')
    .replace(/\u00a0/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&gt;/gi, '>')
    .replace(/&lt;/gi, '<')
    .replace(/&amp;/gi, '&')
    .replace(/\s+/g, ' ')
    .trim();
}

function deepCleanCalendar(obj) {
  if (obj == null) return obj;
  if (typeof obj === 'string') return decodeUnicodeEscapes(obj);
  if (Array.isArray(obj)) {
    return obj
      .map(deepCleanCalendar)
      .filter((x) => x != null && (typeof x !== 'object' || Object.keys(x).length > 0));
  }
  if (typeof obj === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(obj)) out[k] = deepCleanCalendar(v);

    // bersihkan history header rows
    if (Array.isArray(out.history)) {
      out.history = out.history.filter((row) => {
        const d = String(row?.date || '').toLowerCase();
        if (!d) return false;
        if (d === 'history' || d === 'date') return false;
        return /[0-9]/.test(d);
      });
    }
    return out;
  }
  return obj;
}



// =========================== author utilities ============================
const AUTHOR_ALLOW = new Set(['cp', 'ayu', 'az', 'azf', 'yds', 'arl', 'alg', 'mrv']);
const AUTHOR_BLACKLIST = new Set([
  'wti', 'brent', 'fed', 'ecb', 'boj', 'pboc', 'fomc', 'cpi', 'pmi', 'gdp', 'usd', 'eur', 'jpy', 'ppi', 'nfp', 'iea', 'opec', 'ath', 'dxy',
  'hkex', 'hsi', 'dax', 'stoxx', 'spx', 'ndx', 'vix', 'tsx', 'nifty', 'sensex', 'eia', 'api', 'bnb', 'ada', 'sol', 'xrp', 'dot', 'doge',
  'tlt', 'ust', 'imf', 'wto', 'who', 'un', 'eu', 'uk', 'us', 'id', 'sg', 'my', 'th', 'vn', 'ph',
  'ai', 'ml', 'nlp', 'fx', 'ipo', 'yoy', 'mom', 'qoq',
  'bps', 'sep', 'boe', 'boc', 'rba', 'rbnz', 'bcb', 'ihk', 'ihp', 'ihsg', 'wib', 'wita', 'wit', 'ttm', 'ytd', 'q1', 'q2', 'q3', 'q4',
  'bri', 'bni', 'btn', 'bsi', 'bca', 'cimb', 'ocbc', 'uob', 'dbs',
  'rabu', 'kamis', 'jumat', 'sabtu', 'minggu', 'senin', 'selasa',
  'news', 'newsmaker', 'source', 'sumber', 'editor', 'desk'
]);
const AUTHOR_ALIASES = { 'cp': 'cp', 'cP': 'cp', 'CP': 'cp', 'ayu': 'ayu', 'ayiu': 'ayu', 'ayi': 'ayu', 'ay': 'ayu', 'ads': 'ayu', 'az': 'az', 'azf': 'azf', 'yds': 'yds', 'arl': 'arl', 'alg': 'alg', 'mrv': 'mrv' };
const AUTHOR_NAME_MAP = { cp: 'Broto', ayu: 'Ayu', az: 'Nova', azf: 'Nova', yds: 'Yudis', arl: 'Arul', alg: 'Burhan', mrv: 'Marvy' };
function normalizeAuthorInitial(raw) {
  if (!raw) return null;
  let s = String(raw).trim();
  const paren = s.match(/\(([a-z]{2,6})\)/i);
  if (paren) s = paren[1];
  s = s.toLowerCase();
  if (AUTHOR_ALIASES[s]) s = AUTHOR_ALIASES[s];
  if (!/^[a-z]{2,6}$/.test(s)) return null;
  if (AUTHOR_BLACKLIST.has(s)) return null;
  if (!AUTHOR_ALLOW.has(s)) return null;
  return s;
}
function toAuthorName(initial) { return AUTHOR_NAME_MAP[(initial || '').toLowerCase()] || null; }
function extractAuthorFromHtml(html = '') {
  const m = String(html).match(/\(([a-z]{2,6})\)/i);
  return normalizeAuthorInitial(m ? m[1] : null);
}
function extractAuthorFromText(raw = '') {
  const m = String(raw).match(/\(([a-z]{2,6})\)/i);
  return normalizeAuthorInitial(m ? m[1] : null);
}
function sanitizeAuthor(raw) {
  if (!raw) return null;
  let s = String(raw).trim().toLowerCase();
  const m = s.match(/^[a-z]{2,6}$/i);
  if (!m) return null;
  s = (AUTHOR_ALIASES[m[0].toLowerCase()] || m[0].toLowerCase());
  if (AUTHOR_BLACKLIST.has(s)) return null;
  return s;
}

// ================================ scraping ================================
const newsCategories = [
  'economic-news/economy',
  'economic-news/fiscal-moneter',
  'market-news/index',
  'market-news/commodity',
  'market-news/currencies',
  'market-news/crypto',
  'analysis/analysis-market',
  'analysis/analysis-opinion',
];

function extractNewsItem($, el, lang = 'en') {
  const $el = $(el);
  const title = normalizeSpace($el.find('h5.card-title a').text());
  const href = $el.find('h5.card-title a').attr('href') || '';
  const link = href ? 'https://www.newsmaker.id' + href : null;
  if (!link || !link.includes(`/index.php/${lang}/`)) return null;

  const imgSrc = $el.find('img.card-img').attr('src');
  const image = imgSrc ? 'https://www.newsmaker.id' + imgSrc : null;
  const category = normalizeSpace($el.find('span.category-label').text());

  let date = '';
  let summary = '';
  $el.find('p.card-text').each((_, p) => {
    const text = normalizeSpace($(p).text());
    if (/\b\d{1,2}\s+\w+\s+\d{4}\b/i.test(text)) date = text;
    else if (!summary) summary = text;
  });

  let author = extractAuthorFromText(summary);
  if (author) summary = summary.replace(/\(([a-z]{2,8})\)/i, '').trim();

  const publishedAt = parsePublishedAt(date, lang) || null;
  if (!title || !link) return null;
  return { title, link, image, category, date, summary, author: author || null, publishedAt };
}

async function fetchNewsDetailSafe(url, lang = 'en') {
  return retryRequest(async () => {
    const { data } = await axios.get(url, { timeout: 180000, headers: makeHtmlHeaders(lang), maxRedirects: 3 });
    const $ = cheerio.load(data);
    if (isWafOrChallenge($)) {
      console.warn(`🛡️ WAF/Challenge at: ${url}`);
      return { text: '', author: null, sourceName: null };
    }

    const $root = $('div.article-content').first();
    const $article = $root.clone();

    let author = null;
    let sourceName = null;

    const $sourceP = $article.find('p').filter((_, el) => /\b(?:source|sumber)\b\s*:/i.test($(el).text())).first();
    if ($sourceP.length) {
      const pHtml = $sourceP.html() || '';
      author = extractAuthorFromHtml(pHtml) || author;
      sourceName = (pHtml.match(/\b(?:Source|Sumber)\s*:?\s*([^<\n\r]+)/i)?.[1] || '').trim() || sourceName;
      $sourceP.remove();
    }

    if (!author) {
      $article.find('p').each((_, p) => {
        if (author) return;
        const a = extractAuthorFromHtml($(p).html() || '');
        if (a) author = a;
      });
    }

    if (!author) {
      const metaAuthor = $('meta[name="author"]').attr('content');
      const a = sanitizeAuthor(metaAuthor);
      if (a) author = a;
    }

    $article.find('p').each((_, p) => {
      const html = $(p).html() || '';
      const cleaned = html
        .replace(/\s*\([a-z]{2,8}\)\s*$/i, '')
        .replace(/\s*\([a-z]{2,8}\)\s*(?=[\.\,\;\:])/gi, '');
      if (cleaned !== html) $(p).html(cleaned);
    });

    const paragraphs = [];
    $article.find('p').each((_, p) => {
      const txt = normalizeSpace($(p).text());
      if (txt) paragraphs.push(txt);
    });
    const plainText = paragraphs.join('\n\n');
    return { text: plainText, author, sourceName };
  }, 3, 1000);
}

function looksIndonesian(s = '') {
  const t = (s || '').toLowerCase();
  const hits = (t.match(/\b(dan|yang|akan|dari|pada|dengan|sebagai)\b/g) || []).length;
  const anti = (t.match(/\b(the|and|of|for|with|as|to)\b/g) || []).length;
  return hits >= anti;
}

async function runParallelWithLimit(tasks, limit = 3) {
  const results = [];
  const executing = new Set();
  for (const task of tasks) {
    const p = Promise.resolve().then(() => task());
    results.push(p);
    executing.add(p);
    const clean = () => executing.delete(p);
    p.then(clean).catch(clean);
    if (executing.size >= limit) await Promise.race(executing);
  }
  return Promise.all(results);
}

// ====== Distributed lock (single impl) ======
async function withLock(key, ttlSeconds, fn) {
  const token = `${Date.now()}-${Math.random()}`;
  const ok = await redis.set(key, token, 'NX', 'EX', ttlSeconds);
  if (!ok) { console.log(`🔒 Skip: lock ${key} active`); return; }
  try { await fn(); }
  finally {
    try {
      const v = await redis.get(key);
      if (v === token) await redis.del(key);
    } catch { }
  }
}

function buildNewsRow(n) {
  let pub = (n.publishedAt instanceof Date && !Number.isNaN(+n.publishedAt))
    ? n.publishedAt
    : parsePublishedAt(n.date, n.language || 'en');
  pub = ensureDate(pub);

  return {
    title: n.title,
    link: n.link,
    image: n.image,
    category: n.category,
    date: n.date,
    summary: n.summary,
    detail: n.detail || '',
    language: n.language || 'en',
    source_name: n.sourceName || 'Newsmaker23',
    source_url: n.link,
    author: n.author || null,
    author_name: n.author_name || toAuthorName(n.author),
    published_at: pub,
    createdAt: pub,
    updatedAt: pub,
  };
}

async function scrapeNewsByLang(lang = 'en') {
  console.log(`🚀 Scraping news (${lang})...`);

  const { Op } = require('sequelize');
  const existing = await News.findAll({ where: { language: lang }, attributes: ['link'], raw: true });
  const existingLinks = new Set(existing.map((r) => r.link));
  const seenLinks = new Set();
  const allNewItems = [];

  for (const cat of newsCategories) {
    let start = 0;
    let emptyStreak = 0;
    const PAGE_SIZE = 10;
    while (true) {
      const url = `https://www.newsmaker.id/index.php/${lang}/${cat}?start=${start}`;
      try {
        const { data } = await retryRequest(
          () => axios.get(url, { timeout: 180000, headers: makeHtmlHeaders(lang), maxRedirects: 3 }),
          3, 1000
        );

        const $ = cheerio.load(data);
        if (isWafOrChallenge($)) {
          console.warn(`🛡️ Blocked page: ${url}`);
          if (++emptyStreak >= 3) break;
          start += PAGE_SIZE;
          continue;
        }

        const items = [];
        $('div.single-news-item').each((_, el) => {
          const item = extractNewsItem($, el, lang);
          if (item) items.push(item);
        });

        const fresh = items.filter((it) => !existingLinks.has(it.link) && !seenLinks.has(it.link));
        fresh.forEach((it) => seenLinks.add(it.link));

        if (fresh.length === 0) {
          if (++emptyStreak >= 3) break;
        } else {
          emptyStreak = 0;

          const detailTasks = fresh.map((it) => async () => {
            const detail = await fetchNewsDetailSafe(it.link, lang);
            const sourceName = detail?.sourceName || 'Newsmaker23';

            let author = detail?.author
              || extractAuthorFromText(it.summary || '')
              || it.author
              || null;
            author = normalizeAuthorInitial(author);

            if (author) console.log(`✍️ author (${author}) -> ${it.link}`);
            if (detail?.sourceName) console.log(`🔗 source "${detail.sourceName}" -> ${it.link}`);

            return {
              ...it,
              detail: detail?.text || '',
              author,
              author_name: toAuthorName(author),
              sourceName,
              publishedAt: it.publishedAt || null,
              language: lang,
            };
          });

          let detailed = (await runParallelWithLimit(detailTasks, 4)).filter(Boolean);
          if (lang === 'id') {
            detailed = detailed.filter((n) => looksIndonesian(n.detail || n.summary || n.title));
          }
          allNewItems.push(...detailed);
        }

        start += PAGE_SIZE;
        await delay(120);
      } catch (e) {
        console.warn(`⚠️ Failed: ${url} | ${e.message}`);
        if (++emptyStreak >= 3) break;
        start += PAGE_SIZE;
        await delay(300);
      }
    }
  }

  if (allNewItems.length > 0) {
    const rows = allNewItems.map(buildNewsRow);

    const UPDATE_COLS = [
      'summary', 'detail', 'author', 'author_name', 'source_name', 'source_url',
      'published_at', 'image', 'category', 'date', 'language', 'title',
      'createdAt', 'updatedAt'
    ];
    const MAX_DETAIL_CHARS = 500_000;
    const BATCH_SIZE = 150;

    const trimmed = rows.map(r => ({ ...r, detail: (r.detail || '').slice(0, MAX_DETAIL_CHARS) }));
    const safeRows = trimmed.map(r => ({
      ...r,
      published_at: ensureDate(r.published_at),
      createdAt: ensureDate(r.createdAt),
      updatedAt: ensureDate(r.updatedAt),
    }));

    for (let i = 0; i < safeRows.length; i += BATCH_SIZE) {
      const chunk = safeRows.slice(i, i + BATCH_SIZE);
      try {
        await News.bulkCreate(chunk, { updateOnDuplicate: UPDATE_COLS, logging: false });

        // === Kirim push untuk item BARU (dedupe via redis) ===
        for (const r of chunk) {
          try {
            const key = r.link || `${r.title}:${+ensureDate(r.published_at)}:${r.language}`;
            if (await alreadyPushed(redis, key)) continue;

            const rowDb = await News.findOne({
              where: { link: r.link },
              attributes: ['id', 'title', 'summary', 'image', 'category', 'language'],
              logging: false
            });
            if (rowDb?.id) {
              await pushNews({
                id: rowDb.id,
                title: rowDb.title || r.title,
                summary: rowDb.summary || r.summary,
                image: rowDb.image || r.image,
                category: rowDb.category || r.category,
                language: rowDb.language || r.language || lang,
              });
            }
          } catch (e) {
            console.error('⚠️ push after insert failed:', e.message);
          }
        }

      } catch (e) {
        console.error('❌ bulkCreate failed, fallback per-row:', e.message);
        for (const r of chunk) {
          try {
            await News.bulkCreate([r], { updateOnDuplicate: UPDATE_COLS, logging: false });

            const key = r.link || `${r.title}:${+ensureDate(r.published_at)}:${r.language}`;
            if (!(await alreadyPushed(redis, key))) {
              const rowDb = await News.findOne({
                where: { link: r.link },
                attributes: ['id', 'title', 'summary', 'image', 'category', 'language'],
                logging: false
              });
              if (rowDb?.id) {
                await pushNews({
                  id: rowDb.id,
                  title: rowDb.title || r.title,
                  summary: rowDb.summary || r.summary,
                  image: rowDb.image || r.image,
                  category: rowDb.category || r.category,
                  language: rowDb.language || r.language || lang,
                });
              }
            }
          } catch (er) {
            console.error('   ↳ Row failed:', {
              link: r.link,
              title: r.title?.slice(0, 120),
              published_at: r.published_at,
              reason: er.message,
            });
          }
        }
      }
    }
  }
} // end scrapeNewsByLang

// ============================== calendar job ==============================
// ====== Calendar URL mapping (fixed) ======
const CAL_URLS = {
  today: 'https://www.newsmaker.id/index.php/en/analysis/economic-calendar',
  this: 'https://www.newsmaker.id/index.php/en/analysis/economic-calendar/marketcalendar?t=r&limitstart=0',
  prev: 'https://www.newsmaker.id/index.php/en/analysis/economic-calendar/marketcalendar?t=p&limitstart=0',
  next: 'https://www.newsmaker.id/index.php/en/analysis/economic-calendar/marketcalendar?t=n&limitstart=0',
};

// ====== Redis key helper ======
const calKey = (t) => `calendar:${t}`;

// ====== In-memory cache (opsional) ======
let calendarCache = {
  today: { updatedAt: null, data: [] },
  this: { updatedAt: null, data: [] },
  prev: { updatedAt: null, data: [] },
  next: { updatedAt: null, data: [] },
};

const CAL_TTL_SECONDS = 60 * 15; // 15 menit

// === Global calendar QUEUE (serialize all) ===
const calQueue = [];
let calRunning = false;

function enqueueCal(taskFn) {
  return new Promise((resolve, reject) => {
    calQueue.push({ taskFn, resolve, reject });
    runCalQueue();
  });
}

async function runCalQueue() {
  if (calRunning) return;
  calRunning = true;
  while (calQueue.length) {
    const { taskFn, resolve, reject } = calQueue.shift();
    try { resolve(await taskFn()); }
    catch (e) { reject(e); }
  }
  calRunning = false;
}

// ——— Core scraper dengan pagination ———
// (refactor: reuse singleton browser via withPage, no launch here)
async function scrapeCalendarPaged(startUrl, { maxPages = 20, pageSize = 20 } = {}) {
  console.log(`🗓️ Scraping (paged): ${startUrl}`);
  const all = [];

  try {
    return await withPage(async (page) => {
      // stealth ringan
      await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
      });
      await page.setExtraHTTPHeaders({ 'accept-language': 'en-US,en;q=0.9' });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

      // helper bikin URL page ke-N
      const makePagedUrl = (u, offset) => {
        const urlObj = new URL(u);
        urlObj.searchParams.delete('limitstart');
        if (offset > 0) urlObj.searchParams.set('start', String(offset));
        else urlObj.searchParams.delete('start');
        return urlObj.toString();
      };

      for (let p = 1, offset = 0; p <= maxPages; p++, offset += pageSize) {
        const url = (p === 1 ? startUrl : makePagedUrl(startUrl, offset));
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

        await page.waitForSelector('table tbody', { timeout: 60000 }).catch(() => { });
        await page.waitForFunction(
          () => document.querySelectorAll('table tbody tr').length >= 0,
          { timeout: 15000 }
        ).catch(() => { });

const res = await page.evaluate(() => {
  const norm = (s) => (s ?? '').replace(/\s+/g, ' ').trim();

  // Ubah "DD-MM-YYYY" → "YYYY-MM-DD" (kalau sudah YYYY-MM-DD biarkan)
  const fmtDate = (d) => {
    const s = norm(d);
    let m = s.match(/^(\d{2})[-\/](\d{2})[-\/](\d{4})$/);     // DD-MM-YYYY
    if (m) return `${m[3]}-${m[2]}-${m[1]}`;
    m = s.match(/^(\d{4})[-\/](\d{2})[-\/](\d{2})$/);          // YYYY-MM-DD
    if (m) return `${m[1]}-${m[2]}-${m[3]}`;
    return s; // fallback
  };

  // Deteksi teks tanggal
  const isDateText = (txt) =>
    /^\d{2}[-/]\d{2}[-/]\d{4}$/.test(txt) || /^\d{4}-\d{2}-\d{2}$/.test(txt);

  // Cari tabel utama kalender
  function findMainTable() {
    const tables = Array.from(document.querySelectorAll('table'));
    for (const tb of tables) {
      const ths = Array.from(tb.querySelectorAll('thead th'))
        .map((th) => norm(th.textContent).toLowerCase());
      const ok =
        ths.includes('time') &&
        ths.includes('country') &&
        ths.includes('impact') &&
        ths.some((t) => t.includes('figure'));
      if (ok && tb.tBodies && tb.tBodies.length > 0) return tb;
    }
    return null;
  }

  const main = findMainTable();
  if (!main) return { items: [], kept: 0 };

  const tbody = main.tBodies[0];
  const rows = Array.from(tbody.rows);
  const out = [];
  let lastEvent = null;

  for (const tr of rows) {
    const tds = tr.cells;

    // ====== ROW DETAIL (accordion) ======
    const isDetailRow =
      tds.length === 1 || Array.from(tds).some((td) => (td.colSpan || 1) > 1);

    if (isDetailRow) {
      if (!lastEvent) continue;

      const wrap = tr.querySelector('.accordion-collapse') || tr;
      const box = wrap.querySelector('.box-cal-detail') || wrap;

      const sections = Array.from(box.querySelectorAll('.mb-3'));
      const details = {};
      for (const sec of sections) {
        const h = sec.querySelector('h5');
        const title = norm(h?.textContent || '').toLowerCase();
        const text = norm(sec.textContent.replace(h?.textContent || '', ''));
        if (!title) continue;
        if (title.includes('sources')) details.sources = text;
        else if (title.includes('measures')) details.measures = text;
        else if (title.includes('usual effect')) details.usualEffect = text;
        else if (title.includes('frequency')) details.frequency = text;
        else if (title.includes('next released')) details.nextReleased = text;
        else if (title.includes('notes')) details.notes = text;
        else if (title.includes('why trader care')) details.whyTraderCare = text;
      }

      const histTable = wrap.querySelector('table');
      if (histTable) {
        const histRows = Array.from(histTable.querySelectorAll('tbody tr'));
        details.history = histRows
          .map((r) => {
            const cs = r.cells;
            const d0 = norm(cs[0]?.textContent || '');
            if (/^(history|date)$/i.test(d0)) return null;
            return {
              date: d0,
              previous: norm(cs[1]?.textContent || ''),
              forecast: norm(cs[2]?.textContent || ''),
              actual: norm(cs[3]?.textContent || ''),
            };
          })
          .filter((x) => x && x.date && /[0-9]/.test(x.date));
      }

      lastEvent.details = details;
      continue;
    }

    // ====== ROW EVENT ======
    if (tds.length >= 4) {
      const c0 = norm(tds[0]?.innerText || '');
      const c1 = norm(tds[1]?.innerText || '');
      const hasDate = isDateText(c0);

      const dateStr = hasDate ? c0 : null;              // "29-09-2025"
      const timeOnly = hasDate ? (c1 || '-') : (c0 || '-'); // "14.00" atau "06.50"

      const idxCurrency = hasDate ? 2 : 1;
      const idxImpact = hasDate ? 3 : 2;
      const idxFigures = hasDate ? 4 : 3;

      const currency = norm(tds[idxCurrency]?.innerText || '-') || '-';

      const impactCell = tds[idxImpact];
      let impact =
        norm(impactCell?.innerText || '') ||
        impactCell?.getAttribute?.('title') ||
        impactCell?.querySelector?.('[title]')?.getAttribute('title') ||
        impactCell?.querySelector?.('img')?.getAttribute('alt') ||
        '-';
      impact = norm(impact);

      const figuresTd = tds[idxFigures];
      const raw = norm(figuresTd?.innerText || '');
      if (!raw || raw === '-') continue;

      const [eventLine, figuresLine] = (figuresTd?.innerText || '').split('\n');
      const event = norm(eventLine);
      if (!event) continue;

      let previous = '-',
        forecast = '-',
        actual = '-';
      if (figuresLine) {
        const prevMatch = figuresLine.match(/Previous:\s*([^|]*)/i);
        const foreMatch = figuresLine.match(/Forecast:\s*([^|]*)/i);
        const actMatch = figuresLine.match(/Actual:\s*([^|]*)/i);
        previous = prevMatch ? norm(prevMatch[1]) : '-';
        forecast = foreMatch ? norm(foreMatch[1]) : '-';
        actual = actMatch ? norm(actMatch[1]) : '-';
      }

      // KUNCI: untuk week tabs, kolom time = "YYYY-MM-DD HH.mm"; untuk today tetap "HH.mm"
      const timeOut = hasDate ? `${fmtDate(dateStr)} ${timeOnly}` : timeOnly;

      const obj = {
        time: timeOut,
        currency,
        impact,
        event,
        previous,
        forecast,
        actual,
      };
      if (hasDate) obj.date = fmtDate(dateStr); // sertakan juga field date terpisah

      out.push(obj);
      lastEvent = obj;
    }
  }

  return { items: out, kept: out.length };
});



        console.log(`📄 Page offset=${(p - 1) * pageSize}: +${res.kept} (total ${all.length + res.kept})`);
        if (res.kept === 0) break;
        all.push(...res.items);
        if (res.kept < pageSize) break;
      }

      return { ok: true, data: all, error: null };
    });
  } catch (err) {
    console.error('❌ scrapeCalendarPaged failed:', err.message);
    return { ok: false, data: [], error: err.message };
  }
}

// ——— Wrapper per-tab (today/this/prev/next) + cleaning & Redis ———
async function scrapeCalendarToday() {
  const result = await scrapeCalendarPaged(CAL_URLS.today, { maxPages: 1 });
  if (!result.ok) throw new Error(result.error || 'Scrape today failed');
  const updatedAt = new Date();
  const cleaned = deepCleanCalendar(result.data);
  calendarCache.today = { updatedAt, data: cleaned };
  await redis.set(
    calKey('today'),
    JSON.stringify({ status: 'success', updatedAt, total: cleaned.length, data: cleaned }),
    'EX', CAL_TTL_SECONDS
  );
  console.log(`✅ Calendar TODAY updated (${cleaned.length} events)`);
  return calendarCache.today;
}

async function scrapeCalendarThisWeek() {
  const result = await scrapeCalendarPaged(CAL_URLS.this, { maxPages: 20, pageSize: 20 });
  if (!result.ok) throw new Error(result.error || 'Scrape this week failed');
  const updatedAt = new Date();
  const cleaned = deepCleanCalendar(result.data);
  calendarCache.this = { updatedAt, data: cleaned };
  await redis.set(calKey('this'),
    JSON.stringify({ status: 'success', updatedAt, total: cleaned.length, data: cleaned }),
    'EX', CAL_TTL_SECONDS
  );
  console.log(`✅ Calendar THIS WEEK updated (${cleaned.length} events)`);
  return calendarCache.this;
}

async function scrapeCalendarPrevWeek() {
  const result = await scrapeCalendarPaged(CAL_URLS.prev, { maxPages: 20, pageSize: 20 });
  if (!result.ok) throw new Error(result.error || 'Scrape prev week failed');
  const updatedAt = new Date();
  const cleaned = deepCleanCalendar(result.data);
  calendarCache.prev = { updatedAt, data: cleaned };
  await redis.set(calKey('prev'),
    JSON.stringify({ status: 'success', updatedAt, total: cleaned.length, data: cleaned }),
    'EX', CAL_TTL_SECONDS
  );
  console.log(`✅ Calendar PREVIOUS WEEK updated (${cleaned.length} events)`);
  return calendarCache.prev;
}

async function scrapeCalendarNextWeek() {
  const result = await scrapeCalendarPaged(CAL_URLS.next, { maxPages: 20, pageSize: 20 });
  if (!result.ok) throw new Error(result.error || 'Scrape next week failed');
  const updatedAt = new Date();
  const cleaned = deepCleanCalendar(result.data);
  calendarCache.next = { updatedAt, data: cleaned };
  await redis.set(calKey('next'),
    JSON.stringify({ status: 'success', updatedAt, total: cleaned.length, data: cleaned }),
    'EX', CAL_TTL_SECONDS
  );
  console.log(`✅ Calendar NEXT WEEK updated (${cleaned.length} events)`);
  return calendarCache.next;
}

// ====== Pre-warm calendar (serial via queue) ======
(async () => {
  try {
    await enqueueCal(() => scrapeCalendarToday());
    await enqueueCal(() => scrapeCalendarThisWeek());
    await enqueueCal(() => scrapeCalendarPrevWeek());
    await enqueueCal(() => scrapeCalendarNextWeek());
    console.log('✅ Pre-warm calendar done');
  } catch (e) {
    console.error('❌ Pre-warm error:', e.message);
  }
})();

// ====== SCHEDULERS (Calendar serialized + single global lock) ======
const CAL_LOCK_KEY = 'lock:cal:GLOBAL';

function scheduleCal(fn) {
  enqueueCal(() => withLock(CAL_LOCK_KEY, 120, fn));
}
setInterval(() => scheduleCal(scrapeCalendarToday), 15 * 60 * 1000);
setInterval(() => scheduleCal(scrapeCalendarThisWeek), 15 * 60 * 1000 + 10_000);
setInterval(() => scheduleCal(scrapeCalendarPrevWeek), 15 * 60 * 1000 + 20_000);
setInterval(() => scheduleCal(scrapeCalendarNextWeek), 15 * 60 * 1000 + 30_000);

// ============================== schedulers (lain) ============================
withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en'));
withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id'));
withLock('lock:hist:all', 3600, () => scrapeAllHistoricalData());

setInterval(() => withLock('lock:hist:all', 3600, () => scrapeAllHistoricalData()), 4 * 60 * 60 * 1000);
setInterval(() => withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en')), 5 * 60 * 1000);
setInterval(() => withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id')), 5 * 60 * 1000);

// ================================ historical ================================
const BASE_URL = 'https://newsmaker.id/index.php/en/historical-data-2';
const MAX_CONCURRENT_SCRAPES = 10;
let cachedSymbols = null;
let cachedSymbolsTimestamp = 0;

function parseNumber(str) {
  if (str === undefined || str === null || str === '') return null;
  str = String(str);
  const cleaned = str.replace(/,/g, '').trim();
  if (cleaned === '' || cleaned === '-') return null;
  const parsed = parseFloat(cleaned);
  return isNaN(parsed) ? null : parsed;
}

async function getAllSymbols() {
  const now = Date.now();
  if (cachedSymbols && now - cachedSymbolsTimestamp) return cachedSymbols;
  const { data } = await axios.get(BASE_URL, { headers: HTML_HEADERS });
  const $ = cheerio.load(data);
  const options = $('select[name="cid"] option');
  const symbols = [];
  options.each((_, el) => {
    const cid = $(el).attr('value');
    const name = $(el).text().trim();
    if (cid && name) symbols.push({ cid, name });
  });
  cachedSymbols = symbols;
  cachedSymbolsTimestamp = now;
  return symbols;
}

async function scrapePageForSymbol(cid, start, retries = 3, backoff = 1000) {
  try {
    const url = `${BASE_URL}?cid=${cid}&period=d&start=${start}`;
    const { data } = await axios.get(url, { timeout: 120000, headers: HTML_HEADERS, maxRedirects: 3 });
    const $ = cheerio.load(data);
    const table = $('table.table.table-striped.table-bordered');
    if (table.length === 0) return [];
    const rows = table.find('tbody tr');
    const result = [];

    rows.each((i, row) => {
      const $row = $(row);
      const cols = $row.find('td');
      let hasColspan = false;
      cols.each((_, col) => { if ($(col).attr('colspan')) { hasColspan = true; return false; } });
      if (hasColspan) {
        const date = cols.first().text().trim();
        const event = cols.last().text().trim();
        result.push({ date, event, open: null, high: null, low: null, close: null, change: null, volume: null, openInterest: null });
        return;
      }
      const textCols = cols.map((_, el) => $(el).text().trim()).get();
      if (!textCols.length) return;
      const rowData = {
        date: textCols[0] || null,
        open: parseNumber(textCols[1]),
        high: parseNumber(textCols[2]),
        low: parseNumber(textCols[3]),
        close: parseNumber(textCols[4]),
        change: textCols[5] || null,
        volume: parseNumber(textCols[6]),
        openInterest: parseNumber(textCols[7]),
      };
      if (rowData.date && (rowData.open !== null || rowData.close !== null || rowData.high !== null || rowData.low !== null)) {
        result.push(rowData);
      }
    });

    return result;
  } catch (err) {
    if (retries > 0) {
      await delay(backoff);
      return scrapePageForSymbol(cid, start, retries - 1, backoff * 2);
    } else {
      console.error(`❌ scrapePageForSymbol failed:`, err.message);
      return [];
    }
  }
}

async function scrapeAllDataForSymbol(cid, maxRows = 5000) {
  const allData = [];
  let start = 0;
  let pageCount = 0;
  let consecutiveEmptyPages = 0;
  const PAGE_SIZE = 8;
  const MAX_PAGES_PER_SYMBOL = 500;

  while (true) {
    if (allData.length >= maxRows) break;
    if (pageCount >= MAX_PAGES_PER_SYMBOL) break;

    const pageData = await scrapePageForSymbol(cid, start);
    if (pageData.length === 0) {
      if (++consecutiveEmptyPages >= 3) break;
    } else {
      consecutiveEmptyPages = 0;
      const remaining = maxRows - allData.length;
      allData.push(...pageData.slice(0, remaining));
      if (allData.length >= maxRows) break;
    }

    if (pageData.length < PAGE_SIZE) break;
    start += PAGE_SIZE;
    pageCount++;
    await delay(100);
  }

  return allData;
}

async function scrapeAllHistoricalData() {
  const symbols = await getAllSymbols();
  const executing = new Set();

  async function runWithLimit(task) {
    while (executing.size >= MAX_CONCURRENT_SCRAPES) await Promise.race(executing);
    const p = task(); executing.add(p); p.finally(() => executing.delete(p));
    return p;
  }

  await Promise.all(
    symbols.map(({ cid, name }) =>
      runWithLimit(async () => {
        const data = await scrapeAllDataForSymbol(cid);
        for (const row of data) {
          try {
            await HistoricalData.findOrCreate({
              where: { symbol: name, date: row.date },
              defaults: {
                event: row.event || null,
                open: row.open, high: row.high, low: row.low, close: row.close,
                change: row.change, volume: row.volume, openInterest: row.openInterest,
              },
            });
          } catch (err) {
            console.error(`❌ save row ${name} (${row.date}):`, err.message);
          }
        }

        const cacheKey = `historical:${name.toLowerCase()}:all`;
        try {
          await redis.set(cacheKey, JSON.stringify({ status: 'success', symbol: name, data, updatedAt: new Date() }), 'EX', 60 * 60 * 2);
        } catch (err) {
          console.error(`❌ Redis cache error for ${name}:`, err.message);
        }
      })
    )
  );

  return true;
}

// ============================= author fallback ============================
async function fillAuthorFromIDIfMissing(data) {
  const row = data.toJSON ? data.toJSON() : { ...data };
  if ((row.language || '').toLowerCase() !== 'en') return row;
  if (row.author || row.author_name) return row;

  const { Op } = require('sequelize');
  let altLink = null;
  if (row.link) altLink = row.link.replace('/index.php/en/', '/index.php/id/').replace('/en/', '/id/');

  let match = null;
  if (altLink) {
    match = await News.findOne({
      where: { language: 'id', link: altLink },
      attributes: ['author', 'author_name'],
      order: [['published_at', 'DESC']],
      logging: false,
    });
  }
  if (!match && row.image) {
    match = await News.findOne({
      where: { language: 'id', image: row.image },
      attributes: ['author', 'author_name'],
      order: [['published_at', 'DESC']],
      logging: false,
    });
  }
  if (!match && row.published_at) {
    const t = new Date(row.published_at);
    const t1 = new Date(t.getTime() - 6 * 60 * 60 * 1000);
    const t2 = new Date(t.getTime() + 6 * 60 * 60 * 1000);
    const where = { language: 'id', published_at: { [Op.between]: [t1, t2] } };
    if (row.category) where.category = row.category;
    if (row.source_name) where.source_name = row.source_name;
    match = await News.findOne({
      where,
      attributes: ['author', 'author_name'],
      order: [['published_at', 'DESC']],
      logging: false,
    });
  }
  if (match) {
    const m = match.toJSON ? match.toJSON() : match;
    row.author = row.author || m.author || null;
    row.author_name = row.author_name || m.author_name || toAuthorName(m.author) || null;
  }
  return row;
}

// ================================== API ==================================
// NEWS EN
app.get('/api/news', async (req, res) => {
  try {
    const { category = 'all', search = '', page = '1', limit = '500', fields = '' } = req.query;
    const p = Math.max(parseInt(page, 10) || 1, 1);
    const l = Math.min(Math.max(parseInt(limit, 10) || 20, 1), 500);
    const attrs = normalizeFields(fields);
    const { Op } = require('sequelize');

    const cutoff = new Date(); cutoff.setMonth(cutoff.getMonth() - 3);
    const where = { language: 'en', published_at: { [Op.gte]: cutoff } };
    if (category !== 'all') where.category = { [Op.like]: `%${category}%` };
    if (search) {
      where[Op.or] = [
        { title: { [Op.like]: `%${search}%` } },
        { summary: { [Op.like]: `%${search}%` } },
        { detail: { [Op.like]: `%${search}%` } },
      ];
    }

    const cacheKey = makeNewsCacheKey({ lang: 'en', category, search, page: p, limit: l, fields: attrs?.join(',') });
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 30);

    const { rows, count } = await News.findAndCountAll({
      where,
      attributes: attrs || undefined,
      order: [['published_at', 'DESC'], ['createdAt', 'DESC']],
      limit: l,
      offset: (p - 1) * l,
    });

    const data = rows.map(r => {
      const row = r.toJSON ? r.toJSON() : r;
      if (!row.author_name) row.author_name = toAuthorName(row.author) || null;
      return row;
    });

    const payload = { status: 'success', page: p, perPage: l, total: count, data };
    await redis.set(cacheKey, JSON.stringify(payload), 'EX', 45);
    return sendWithETag(req, res, payload, 30);
  } catch (err) {
    console.error('❌ /api/news error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// NEWS ID
app.get('/api/news-id', async (req, res) => {
  try {
    const { category = 'all', search = '', page = '1', limit = '500', fields = '' } = req.query;
    const p = Math.max(parseInt(page, 10) || 1, 1);
    const l = Math.min(Math.max(parseInt(limit, 10) || 20, 1), 500);
    const attrs = normalizeFields(fields);
    const { Op } = require('sequelize');

    const cutoff = new Date(); cutoff.setMonth(cutoff.getMonth() - 3);
    const where = { language: 'id', published_at: { [Op.gte]: cutoff } };
    if (category !== 'all') where.category = { [Op.like]: `%${category}%` };
    if (search) {
      where[Op.or] = [
        { title: { [Op.like]: `%${search}%` } },
        { summary: { [Op.like]: `%${search}%` } },
        { detail: { [Op.like]: `%${search}%` } },
      ];
    }

    const cacheKey = makeNewsCacheKey({ lang: 'id', category, search, page: p, limit: l, fields: attrs?.join(',') });
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 30);

    const { rows, count } = await News.findAndCountAll({
      where,
      attributes: attrs || undefined,
      order: [['published_at', 'DESC'], ['createdAt', 'DESC']],
      limit: l,
      offset: (p - 1) * l,
    });

    const data = rows.map(r => {
      const row = r.toJSON ? r.toJSON() : r;
      if (!row.author_name) row.author_name = toAuthorName(row.author) || null;
      return row;
    });

    const payload = { status: 'success', page: p, perPage: l, total: count, data };
    await redis.set(cacheKey, JSON.stringify(payload), 'EX', 45);
    return sendWithETag(req, res, payload, 30);
  } catch (err) {
    console.error('❌ /api/news-id error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// NEWS detail EN
app.get('/api/news/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });
    const cacheKey = `news:item:${id}`;
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 120);

    const row = await News.findByPk(id);
    if (!row) return res.status(404).json({ error: 'Not found' });

    let data = row.toJSON ? row.toJSON() : row;
    if (!data.author_name) data.author_name = toAuthorName(data.author) || null;
    data = await fillAuthorFromIDIfMissing(data);

    const payload = { status: 'success', data };
    await redis.set(cacheKey, JSON.stringify(payload), 'EX', 120);
    return sendWithETag(req, res, payload, 120);
  } catch (err) {
    console.error('❌ /api/news/:id error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// NEWS detail ID
app.get('/api/news-id/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });
    const cacheKey = `news:item:${id}`;
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 120);

    const row = await News.findByPk(id);
    if (!row) return res.status(404).json({ error: 'Not found' });

    const data = row.toJSON ? row.toJSON() : row;
    if (!data.author_name) data.author_name = toAuthorName(data.author) || null;

    const payload = { status: 'success', data };
    await redis.set(cacheKey, JSON.stringify(payload), 'EX', 120);
    return sendWithETag(req, res, payload, 120);
  } catch (err) {
    console.error('❌ /api/news-id/:id error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ================================ Calendar API =============================
// TODAY
app.get('/api/calendar/today', async (req, res) => {
  try {
    const cached = await redis.get(calKey('today'));
    if (cached) {
      const payload = JSON.parse(cached);
      payload.data = deepCleanCalendar(payload.data);
      payload.total = Array.isArray(payload.data) ? payload.data.length : 0;
      return res.json(payload);
    }
    const { updatedAt, data } = await enqueueCal(() => scrapeCalendarToday());
    const payload = { status: 'success', updatedAt, total: data.length, data: deepCleanCalendar(data) };
    await redis.set(calKey('today'), JSON.stringify(payload), 'EX', 60 * 60);
    res.json(payload);
  } catch (err) {
    console.error('❌ /api/calendar/today error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// THIS WEEK
app.get('/api/calendar/this-week', async (req, res) => {
  try {
    const cached = await redis.get(calKey('this'));
    if (cached) {
      const payload = JSON.parse(cached);
      payload.data = deepCleanCalendar(payload.data);
      payload.total = Array.isArray(payload.data) ? payload.data.length : 0;
      return res.json(payload);
    }
    const { updatedAt, data } = await enqueueCal(() => scrapeCalendarThisWeek());
    const payload = { status: 'success', updatedAt, total: data.length, data: deepCleanCalendar(data) };
    await redis.set(calKey('this'), JSON.stringify(payload), 'EX', 60 * 60);
    res.json(payload);
  } catch (err) {
    console.error('❌ /api/calendar/this-week error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PREVIOUS WEEK
app.get('/api/calendar/previous-week', async (req, res) => {
  try {
    const cached = await redis.get(calKey('prev'));
    if (cached) {
      const payload = JSON.parse(cached);
      payload.data = deepCleanCalendar(payload.data);
      payload.total = Array.isArray(payload.data) ? payload.data.length : 0;
      return res.json(payload);
    }
    const { updatedAt, data } = await enqueueCal(() => scrapeCalendarPrevWeek());
    const payload = { status: 'success', updatedAt, total: data.length, data: deepCleanCalendar(data) };
    await redis.set(calKey('prev'), JSON.stringify(payload), 'EX', 60 * 60);
    res.json(payload);
  } catch (err) {
    console.error('❌ /api/calendar/previous-week error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// NEXT WEEK
app.get('/api/calendar/next-week', async (req, res) => {
  try {
    const cached = await redis.get(calKey('next'));
    if (cached) {
      const payload = JSON.parse(cached);
      payload.data = deepCleanCalendar(payload.data);
      payload.total = Array.isArray(payload.data) ? payload.data.length : 0;
      return res.json(payload);
    }
    const { updatedAt, data } = await enqueueCal(() => scrapeCalendarNextWeek());
    const payload = { status: 'success', updatedAt, total: data.length, data: deepCleanCalendar(data) };
    await redis.set(calKey('next'), JSON.stringify(payload), 'EX', 60 * 60);
    res.json(payload);
  } catch (err) {
    console.error('❌ /api/calendar/next-week error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Legacy param ?t=...
app.get('/api/calendar', async (req, res) => {
  try {
    const tParam = (req.query.t || 'today').toString().toLowerCase();
    if (tParam === 'today') return res.redirect(307, '/api/calendar/today');
    if (tParam === 'this') return res.redirect(307, '/api/calendar/this-week');
    if (tParam === 'prev') return res.redirect(307, '/api/calendar/previous-week');
    if (tParam === 'next') return res.redirect(307, '/api/calendar/next-week');
    return res.redirect(307, '/api/calendar/today');
  } catch (err) {
    console.error('❌ /api/calendar (legacy) error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/calendar/all', async (req, res) => {
  try {
    const keys = ['today', 'this', 'prev', 'next'];
    const cached = await Promise.all(keys.map(k => redis.get(calKey(k))));
    const result = {};
    const toFetch = [];

    keys.forEach((k, i) => {
      if (cached[i]) result[k] = JSON.parse(cached[i]);
      else toFetch.push(k);
    });

    for (const k of toFetch) {
      let dataPack;
      if (k === 'today') dataPack = await enqueueCal(() => scrapeCalendarToday());
      else if (k === 'this') dataPack = await enqueueCal(() => scrapeCalendarThisWeek());
      else if (k === 'prev') dataPack = await enqueueCal(() => scrapeCalendarPrevWeek());
      else if (k === 'next') dataPack = await enqueueCal(() => scrapeCalendarNextWeek());

      result[k] = { status: 'success', updatedAt: dataPack.updatedAt, total: dataPack.data.length, data: dataPack.data };
      await redis.set(calKey(k), JSON.stringify(result[k]), 'EX', 60 * 60);
    }

    res.json({ status: 'success', ...result });
  } catch (err) {
    console.error('❌ /api/calendar/all error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/historical', async (req, res) => {
  try {
    const { QueryTypes } = require('sequelize');
    const tableName = HistoricalData.getTableName().toString();

    const orderedSymbols = await sequelize.query(`
      SELECT symbol, latestDate, updatedAtMax
      FROM (
        SELECT
          symbol,
          MAX(STR_TO_DATE(\`date\`, '%d %b %Y')) AS latestDate,
          MAX(updatedAt) AS updatedAtMax
        FROM \`${tableName}\`
        GROUP BY symbol
      ) AS t
      ORDER BY (latestDate IS NULL), latestDate DESC, updatedAtMax DESC
    `, { type: QueryTypes.SELECT });

    if (!orderedSymbols.length) {
      return res.status(404).json({ status: 'empty', message: 'No historical data found.' });
    }

    const allData = [];
    for (const row of orderedSymbols) {
      const symbol = row.symbol;
      const rows = await HistoricalData.findAll({
        where: { symbol },
        order: [
          [sequelize.literal("(STR_TO_DATE(`date`, '%d %b %Y') IS NULL)"), 'ASC'],
          [sequelize.literal("STR_TO_DATE(`date`, '%d %b %Y')"), 'DESC'],
          ['updatedAt', 'DESC'],
        ],
        raw: true,
      });
      allData.push({ symbol, data: rows, updatedAt: row.updatedAtMax || null });
    }

    return res.json({ status: 'success', totalSymbols: allData.length, data: allData });
  } catch (err) {
    console.error('❌ /api/historical error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ================================ Quotes ==================================
app.get('/api/quotes', async (req, res) => {
  try {
    const url = 'https://www.newsmaker.id/quotes/live?s=LGD+LSI+GHSIU5+LCOPX5+SN1U5+DJIA+DAX+DX+AUDUSD+EURUSD+GBPUSD+CHF+JPY+RP';
    const { data } = await axios.get(url, { timeout: 15000 });
    const quotes = [];
    for (let i = 1; i <= data[0].count; i++) {
      const high = data[i].high !== 0 ? data[i].high : data[i].last;
      const low = data[i].low !== 0 ? data[i].low : data[i].last;
      const open = data[i].open !== 0 ? data[i].open : data[i].last;
      quotes.push({
        symbol: data[i].symbol,
        last: data[i].last,
        high, low, open,
        prevClose: data[i].prevClose,
        valueChange: data[i].valueChange,
        percentChange: data[i].percentChange,
      });
    }
    return res.json({ status: 'success', updatedAt: new Date(), total: quotes.length, data: quotes, source: 'live' });
  } catch (err) {
    console.error('❌ /api/quotes error:', err.message);
    if (Array.isArray(cachedQuotes) && cachedQuotes.length > 0) {
      const validQuotes = cachedQuotes.map((q) => ({
        ...q,
        high: q.high !== 0 ? q.high : q.last,
        low: q.low !== 0 ? q.low : q.last,
        open: q.open !== 0 ? q.open : q.last,
      }));
      return res.json({ status: 'degraded', updatedAt: lastUpdatedQuotes, total: validQuotes.length, data: validQuotes, source: 'fallback-cache' });
    }
    return res.status(502).json({ status: 'error', message: 'Failed to fetch live quotes', detail: err.message });
  }
});

// ================================ Shorts (YouTube RSS) ====================
const ytCache = new NodeCache({ stdTTL: 300, checkperiod: 60 });
function ytBuildFeedUrl({ channelId, user, playlistId }) {
  if (channelId) return `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
  if (user) return `https://www.youtube.com/feeds/videos.xml?user=${user}`;
  if (playlistId) return `https://www.youtube.com/feeds/videos.xml?playlist_id=${playlistId}`;
  return null;
}
async function ytResolveHandleToChannelId(handleRaw) {
  const handle = (handleRaw || '').startsWith('@') ? handleRaw.slice(1) : String(handleRaw || '').trim();
  if (!handle) throw new Error('Handle kosong');
  const url = `https://www.youtube.com/@${handle}`;
  const cacheKey = `yt:handle2cid:${handle}`;
  const memHit = ytCache.get(cacheKey);
  if (memHit) return memHit;
  const resp = await axios.get(url, { timeout: 15000, headers: HTML_HEADERS, maxRedirects: 2, responseType: 'text' });
  const html = resp.data || '';
  let m = html.match(/"channelId"\s*:\s*"(?<cid>UC[0-9A-Za-z_-]{20,})"/);
  if (!m || !m.groups?.cid) {
    const b = html.match(/"browseId"\s*:\s*"(?<cid>UC[0-9A-Za-z_-]{20,})"/);
    if (!b || !b.groups?.cid) throw new Error('Gagal menemukan channelId dari handle');
    m = b;
  }
  const cid = m.groups.cid;
  ytCache.set(cacheKey, cid, 3600);
  return cid;
}
function ytExtractShorts(xml, { maxDuration = 61, guessShortsIfNoDuration = true } = {}) {
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_', removeNSPrefix: false });
  const json = parser.parse(xml || '');
  const feed = json?.feed || {};
  const entries = Array.isArray(feed.entry) ? feed.entry : feed.entry ? [feed.entry] : [];
  const list = entries.map((e) => {
    const videoId = e['yt:videoId'];
    const title = typeof e.title === 'string' ? e.title : e.title?.['#text'] || '';
    const link = (Array.isArray(e.link) ? e.link.find(l => l['@_rel'] === 'alternate') : e.link)?.['@_href']
      || (videoId ? `https://www.youtube.com/watch?v=${videoId}` : '');
    const mg = e['media:group'] || {};
    const thumb = (Array.isArray(mg['media:thumbnail']) ? mg['media:thumbnail'][0] : mg['media:thumbnail'])?.['@_url'] || null;
    let durationSeconds = mg['yt:duration']?.['@_seconds'] != null ? Number(mg['yt:duration']['@_seconds']) : null;
    if (durationSeconds == null) {
      const mc = Array.isArray(mg['media:content']) ? mg['media:content'][0] : mg['media:content'];
      const alt = mc?.['@_duration'] != null ? Number(mc['@_duration']) : null;
      if (Number.isFinite(alt)) durationSeconds = alt;
    }
    if (!Number.isFinite(durationSeconds)) durationSeconds = null;
    const description = typeof mg['media:description'] === 'string' ? mg['media:description'] : mg['media:description']?.['#text'] || '';
    const looksLikeShorts = /#shorts/i.test(`${title} ${description}`) || /\/shorts\//i.test(link);
    return { videoId, title, url: link, publishedAt: e.published, thumbnail: thumb, durationSeconds, looksLikeShorts };
  }).filter((it) => (it.durationSeconds != null ? it.durationSeconds <= maxDuration : (guessShortsIfNoDuration ? it.looksLikeShorts : false)));

  return { meta: { title: feed?.title ?? null, author: feed?.author?.name ?? null, total: list.length }, items: list };
}
app.get('/api/shorts', async (req, res) => {
  try {
    let { handle, channelId, user, playlistId } = req.query;
    const limit = Math.max(parseInt(req.query.limit, 10) || 20, 1);
    const maxDuration = Math.max(parseInt(req.query.maxDuration, 10) || 61, 1);
    const guess = String(req.query.guess || '1') === '1';
    const cacheTtl = parseInt(req.query.cacheTtl, 10) || 300;

    if (!handle && !channelId && !user && !playlistId) {
      return res.status(400).json({ ok: false, error: 'Sertakan salah satu: handle / channelId / user / playlistId', example: '/api/shorts?handle=NewsMaker23&limit=15&guess=1' });
    }
    if (handle && !channelId) channelId = await ytResolveHandleToChannelId(handle);

    const feedUrl = ytBuildFeedUrl({ channelId, user, playlistId });
    if (!feedUrl) return res.status(400).json({ ok: false, error: 'Parameter tidak valid' });

    const rKey = `yt:shorts:${feedUrl}:max${maxDuration}:guess${guess ? 1 : 0}`;
    const memHit = ytCache.get(rKey);
    if (memHit) {
      return sendWithETag(req, res, { ok: true, source: 'cache-mem', feedUrl, count: Math.min(memHit.items.length, limit), meta: memHit.meta, data: memHit.items.slice(0, limit) }, 60);
    }

    try {
      const r = await redis.get(rKey);
      if (r) {
        const parsed = JSON.parse(r);
        ytCache.set(rKey, parsed, cacheTtl);
        return sendWithETag(req, res, { ok: true, source: 'cache-redis', feedUrl, count: Math.min(parsed.items.length, limit), meta: parsed.meta, data: parsed.items.slice(0, limit) }, 60);
      }
    } catch (e) {
      console.warn('⚠️ Redis get error (shorts):', e.message);
    }

    const { data: xml } = await axios.get(feedUrl, { timeout: 15000, responseType: 'text' });
    const parsed = ytExtractShorts(xml, { maxDuration, guessShortsIfNoDuration: guess });

    ytCache.set(rKey, parsed, cacheTtl);
    try { await redis.set(rKey, JSON.stringify(parsed), 'EX', cacheTtl); } catch (e) { console.warn('⚠️ Redis set error (shorts):', e.message); }

    return sendWithETag(req, res, { ok: true, source: 'live', feedUrl, count: Math.min(parsed.items.length, limit), meta: parsed.meta, data: parsed.items.slice(0, limit) }, 60);
  } catch (err) {
    console.error('❌ /api/shorts error:', err.message);
    res.status(500).json({ ok: false, error: 'Internal server error', detail: err.message });
  }
});

// =============================== misc API ================================
app.delete('/api/cache', async (req, res) => {
  try {
    const { pattern } = req.query;
    const keyPattern = pattern || 'historical:*';
    const keys = await redis.keys(keyPattern);
    if (keys.length === 0) return res.json({ message: 'No cache keys found.' });
    await redis.del(...keys);
    res.json({ message: `🧹 ${keys.length} cache key(s) deleted.` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/status', (req, res) => {
  res.json({
    newsCount: cachedNews.length,
    newsIDCount: cachedNewsID.length,
    calendarCount: cachedCalendar.length,
    quotesCount: cachedQuotes.length,
    lastUpdated: { news: lastUpdatedNews, newsID: lastUpdatedNewsID, calendar: lastUpdatedCalendar, quotes: lastUpdatedQuotes },
  });
});

app.get('/healthz', async (req, res) => {
  try {
    await sequelize.query('SELECT 1+1 AS ok');
    await redis.ping();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// === Endpoint uji kirim push manual (sekarang dukung language) ===
app.post('/api/test-push', async (req, res) => {
  try {
    const {
      id = 999,
      title = 'Test Push',
      summary = 'Halo dari server',
      image = '',
      category = 'general',
      language = 'id',
    } = req.body || {};
    await pushNews({ id, title, summary, image, category, language });
    res.json({ ok: true, topics: [`news_${resolveLang(language)}`, `${topicBaseFor(category)}_${resolveLang(language)}`] });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ============================== start server =============================
const server = app.listen(PORT, () => {
  console.log(`🚀 Server ready at http://0.0.0.0:${PORT}`);
});

// graceful shutdown (Railway friendly)
function shutdown(sig) {
  console.log(`\n${sig} received. Shutting down...`);
  server.close(async () => {
    try { await sequelize.close(); } catch { }
    try { await redis.quit(); } catch { }
    try { await closeBrowser(); } catch { }
    process.exit(0);
  });
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));


