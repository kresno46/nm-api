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

// map kategori → topic (ubah sesuai taksonomi kamu)
function topicFor(category = '') {
  const c = (category || '').toLowerCase();
  if (c.includes('commodity')) return 'news_commodity';
  if (c.includes('currenc'))  return 'news_currencies';
  if (c.includes('index'))    return 'news_index';
  if (c.includes('crypto'))   return 'news_crypto';
  if (c.includes('economy') || c.includes('fiscal')) return 'news_economy';
  if (c.includes('analysis')) return 'news_analysis';
  return 'news_all';
}

// dedupe push dgn redis (3 hari)
async function alreadyPushed(redis, key) {
  const k = `push:sent:${key}`;
  const ok = await redis.set(k, '1', 'NX', 'EX', 60 * 60 * 24 * 3);
  return ok === null; // true artinya SUDAH ada
}

async function pushNews({ id, title, summary, image, category }) {
  const topic = topicFor(category);
  const deeplink = `newsmaker23://news?id=${id}`;
  const collapseKey = `news_${id}`;
  const msg = {
    topic,
    notification: {
      title: String(title || '').slice(0, 110),
      body: String(summary || 'Tap to read more').slice(0, 230),
      image: image || undefined,
    },
    data: {
      news_id: String(id),
      url: deeplink,
      category: String(category || ''),
      click_action: 'FLUTTER_NOTIFICATION_CLICK',
    },
    android: {
      notification: { channelId: 'high_importance_channel', imageUrl: image || undefined, priority: 'HIGH', defaultSound: true },
      collapseKey,
    },
    apns: { payload: { aps: { sound: 'default' } }, fcmOptions: { imageUrl: image || undefined } },
  };
  const res = await fcm.send(msg);
  console.log('📣 Push sent:', res, '→', topic);
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
const MONTHS_EN = { jan:0,january:0,feb:1,february:1,mar:2,march:2,apr:3,april:3,may:4,jun:5,june:5,jul:6,july:6,aug:7,august:7,sep:8,sept:8,september:8,oct:9,october:9,nov:10,november:10,dec:11,december:11 };
const MONTHS_ID = { jan:0,januari:0,feb:1,februari:1,mar:2,maret:2,apr:3,april:3,mei:4,jun:5,juni:5,jul:6,juli:6,agu:7,agustus:7,agst:7,sep:8,september:8,okt:9,oktober:9,nov:10,november:10,des:11,desember:11 };

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
const NEWS_ALLOWED_FIELDS = new Set(['id','title','link','image','category','date','summary','detail','language','createdAt','source_name','source_url','author','author_name','published_at']);
function normalizeFields(fields) {
  if (!fields) return null;
  const arr = fields.split(',').map(s => s.trim()).filter(Boolean);
  const picked = arr.filter(f => NEWS_ALLOWED_FIELDS.has(f));
  return picked.length ? picked : Array.from(NEWS_ALLOWED_FIELDS);
}

// =========================== author utilities ============================
const AUTHOR_ALLOW = new Set(['cp','ayu','az','azf','yds','arl','alg','mrv']);
const AUTHOR_BLACKLIST = new Set([
  'wti','brent','fed','ecb','boj','pboc','fomc','cpi','pmi','gdp','usd','eur','jpy','ppi','nfp','iea','opec','ath','dxy',
  'hkex','hsi','dax','stoxx','spx','ndx','vix','tsx','nifty','sensex','eia','api','bnb','ada','sol','xrp','dot','doge',
  'tlt','ust','imf','wto','who','un','eu','uk','us','id','sg','my','th','vn','ph',
  'ai','ml','nlp','fx','ipo','yoy','mom','qoq',
  'bps','sep','boe','boc','rba','rbnz','bcb','ihk','ihp','ihsg','wib','wita','wit','ttm','ytd','q1','q2','q3','q4',
  'bri','bni','btn','bsi','bca','cimb','ocbc','uob','dbs',
  'rabu','kamis','jumat','sabtu','minggu','senin','selasa',
  'news','newsmaker','source','sumber','editor','desk'
]);
const AUTHOR_ALIASES = { 'cp':'cp','cP':'cp','CP':'cp','ayu':'ayu','ayiu':'ayu','ayi':'ayu','ay':'ayu','ads':'ayu','az':'az','azf':'azf','yds':'yds','arl':'arl','alg':'alg','mrv':'mrv' };
const AUTHOR_NAME_MAP = { cp:'Broto', ayu:'Ayu', az:'Nova', azf:'Nova', yds:'Yudis', arl:'Arul', alg:'Burhan', mrv:'Marvy' };
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

async function withLock(lockKey, ttlSec, fn) {
  const ok = await redis.set(lockKey, Date.now(), 'NX', 'EX', ttlSec);
  if (!ok) { console.log(`🔒 Skip: lock ${lockKey} active`); return; }
  try { await fn(); }
  finally { try { await redis.del(lockKey); } catch { /* no-op */ } }
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
      'summary','detail','author','author_name','source_name','source_url',
      'published_at','image','category','date','language','title',
      'createdAt','updatedAt'
    ];
    const MAX_DETAIL_CHARS = 500_000;
    const BATCH_SIZE = 150;

    const trimmed = rows.map(r => ({ ...r, detail: (r.detail || '').slice(0, MAX_DETAIL_CHARS) }));
    const safeRows = trimmed.map(r => ({
      ...r,
      published_at: ensureDate(r.published_at),
      createdAt:    ensureDate(r.createdAt),
      updatedAt:    ensureDate(r.updatedAt),
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
              where: { link: r.link }, attributes: ['id','title','summary','image','category'], logging: false
            });
            if (rowDb?.id) {
              await pushNews({
                id: rowDb.id,
                title: rowDb.title || r.title,
                summary: rowDb.summary || r.summary,
                image: rowDb.image || r.image,
                category: rowDb.category || r.category,
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

            // kirim push per-row juga
            const key = r.link || `${r.title}:${+ensureDate(r.published_at)}:${r.language}`;
            if (!(await alreadyPushed(redis, key))) {
              const rowDb = await News.findOne({
                where: { link: r.link }, attributes: ['id','title','summary','image','category'], logging:false
              });
              if (rowDb?.id) {
                await pushNews({
                  id: rowDb.id,
                  title: rowDb.title || r.title,
                  summary: rowDb.summary || r.summary,
                  image: rowDb.image || r.image,
                  category: rowDb.category || r.category,
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
async function scrapeCalendar() {
  console.log('🗓️ Scraping calendar with Puppeteer...');
  let browser;
  try {
    browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36');
    await page.goto('https://www.newsmaker.id/index.php/en/analysis/economic-calendar', { waitUntil: 'networkidle2', timeout: 60000 });
    await page.waitForSelector('table');

    const eventsData = await page.evaluate(() => {
      const rows = Array.from(document.querySelectorAll('table tbody tr'));
      const results = [];
      for (let i = 0; i < rows.length; i++) {
        const tds = rows[i].querySelectorAll('td');
        if (tds.length < 4) continue;
        const time = tds[0].innerText.trim();
        const currency = tds[1].innerText.trim();
        const impactSpan = tds[2].querySelector('span');
        const impact = impactSpan ? impactSpan.innerText.trim() : null;
        const raw = tds[3].innerText.trim();
        if (!time || !currency || !impact || !raw || raw === '-' || currency === '-' || raw.includes('2025-')) continue;

        const [eventLine, figuresLine] = raw.split('\n');
        const event = eventLine?.trim() || null;
        let previous = null, forecast = null, actual = null;
        if (figuresLine) {
          const prevMatch = figuresLine.match(/Previous:\s*([^|]*)/);
          const foreMatch = figuresLine.match(/Forecast:\s*([^|]*)/);
          const actMatch = figuresLine.match(/Actual:\s*([^|]*)/);
          previous = prevMatch ? prevMatch[1].trim() : '-';
          forecast = foreMatch ? foreMatch[1].trim() : '-';
          actual = actMatch ? actMatch[1].trim() : '-';
        }
        results.push({ time, currency, impact, event, previous, forecast, actual });
      }
      return results;
    });

    cachedCalendar = eventsData;
    lastUpdatedCalendar = new Date();
    await browser.close();
    console.log(`✅ Calendar updated (${cachedCalendar.length} events)`);

    try {
      await redis.set('calendar:all', JSON.stringify({
        status: 'success',
        updatedAt: lastUpdatedCalendar,
        total: cachedCalendar.length,
        data: cachedCalendar,
      }), 'EX', 60 * 15);
      console.log('💾 Calendar cached in Redis (15m)');
    } catch (err) {
      console.error('❌ Redis save calendar:', err.message);
    }
  } catch (err) {
    if (browser) await browser.close();
    console.error('❌ Calendar scraping failed:', err.message);
  }
}

// =========================== historical scraper ===========================
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
        result.push({ date, event, open:null, high:null, low:null, close:null, change:null, volume:null, openInterest:null });
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
      attributes: ['author','author_name'],
      order: [['published_at', 'DESC']],
      logging: false,
    });
  }
  if (!match && row.image) {
    match = await News.findOne({
      where: { language: 'id', image: row.image },
      attributes: ['author','author_name'],
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
      attributes: ['author','author_name'],
      order: [['published_at','DESC']],
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

// ============================== schedulers ================================
withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en'));
withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id'));
scrapeCalendar();
withLock('lock:hist:all', 3600, () => scrapeAllHistoricalData());

setInterval(() => withLock('lock:hist:all', 3600, () => scrapeAllHistoricalData()), 4 * 60 * 60 * 1000);
setInterval(() => withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en')), 10 * 60 * 1000);
setInterval(() => withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id')), 10 * 60 * 1000);
setInterval(scrapeCalendar, 60 * 60 * 1000);

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
        { title:   { [Op.like]: `%${search}%` } },
        { summary: { [Op.like]: `%${search}%` } },
        { detail:  { [Op.like]: `%${search}%` } },
      ];
    }

    const cacheKey = makeNewsCacheKey({ lang:'en', category, search, page:p, limit:l, fields: attrs?.join(',') });
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 30);

    const { rows, count } = await News.findAndCountAll({
      where,
      attributes: attrs || undefined,
      order: [['published_at','DESC'],['createdAt','DESC']],
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
        { title:   { [Op.like]: `%${search}%` } },
        { summary: { [Op.like]: `%${search}%` } },
        { detail:  { [Op.like]: `%${search}%` } },
      ];
    }

    const cacheKey = makeNewsCacheKey({ lang:'id', category, search, page:p, limit:l, fields: attrs?.join(',') });
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 30);

    const { rows, count } = await News.findAndCountAll({
      where,
      attributes: attrs || undefined,
      order: [['published_at','DESC'],['createdAt','DESC']],
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

// Calendar API
app.get('/api/calendar', async (req, res) => {
  try {
    const cached = await redis.get('calendar:all');
    if (cached) return res.json(JSON.parse(cached));
    await scrapeCalendar();
    const freshData = {
      status: 'success',
      updatedAt: lastUpdatedCalendar,
      total: cachedCalendar.length,
      data: cachedCalendar,
    };
    await redis.set('calendar:all', JSON.stringify(freshData), 'EX', 60 * 60);
    res.json(freshData);
  } catch (err) {
    console.error('❌ /api/calendar error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Historical API
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

// Quotes
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

// Shorts (YouTube RSS) – dipertahankan
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

// === Endpoint uji kirim push manual ===
app.post('/api/test-push', async (req, res) => {
  try {
    const { id = 999, title = 'Test Push', summary = 'Halo dari server', image = '', category = 'general' } = req.body || {};
    await pushNews({ id, title, summary, image, category });
    res.json({ ok: true });
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
    try { await sequelize.close(); } catch {}
    try { await redis.quit(); } catch {}
    process.exit(0);
  });
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
