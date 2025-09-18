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

app.use(cors());
app.use(helmet());
app.use(compression());
app.use(rateLimit({ windowMs: 60_000, max: 120 })); // 120 req/menit/IP

// ===== In-memory caches =====
let cachedNews = [];
let cachedNewsID = [];
let cachedCalendar = [];
let cachedQuotes = [];
let lastUpdatedNews = null;
let lastUpdatedNewsID = null;
let lastUpdatedCalendar = null;
let lastUpdatedQuotes = null;

// ===== Redis (init lebih awal, sebelum dipakai) =====
const redis = new Redis(process.env.REDIS_PUBLIC_URL || {
  host: '127.0.0.1',
  port: 6379,
  retryStrategy: (times) => Math.min(times * 50, 2000),
});
redis.on('connect', () => console.log('✅ Redis connected'));
redis.on('error', (err) => console.error('❌ Redis error:', err));

// ===== DB init =====
sequelize.sync().then(() => {
  console.log('📦 Database News & NewsID synced');
  console.log('📦 Database synced (include HistoricalData)');
});

(async () => {
  try {
    await sequelize.authenticate();
    console.log('✅ MySQL connected successfully!');
  } catch (err) {
    console.error('❌ MySQL connection error:', err.message);
  }

  try {
    await redis.set('test_key', 'hello_redis');
    const val = await redis.get('test_key');
    console.log('🔁 Redis check:', val);
  } catch (err) {
    console.error('❌ Redis connection error:', err.message);
  }
})();

// ===== Helpers =====

const MONTHS_EN = {
  jan: 0, january: 0,
  feb: 1, february: 1,
  mar: 2, march: 2,
  apr: 3, april: 3,
  may: 4,
  jun: 5, june: 5,
  jul: 6, july: 6,
  aug: 7, august: 7,
  sep: 8, sept: 8, september: 8,
  oct: 9, october: 9,
  nov: 10, november: 10,
  dec: 11, december: 11,
};

const MONTHS_ID = {
  jan: 0, januari: 0,
  feb: 1, februari: 1,
  mar: 2, maret: 2,
  apr: 3, april: 3,
  mei: 4,
  jun: 5, juni: 5,
  jul: 6, juli: 6,
  agu: 7, agustus: 7, agst: 7,
  sep: 8, september: 8,
  okt: 9, oktober: 9,
  nov: 10, november: 10,
  des: 11, desember: 11,
};

function parsePublishedAt(dateStr = '', lang = 'en') {
  const s = String(dateStr).trim().replace(/\s+/g, ' ');
  if (!s) return null;

  // Dukung "12 September 2025" ATAU "12 September 2025 08:38"
  const m = s.match(/^(\d{1,2})\s+([A-Za-z\.]+)\s+(\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
  if (!m) return null;

  const day = parseInt(m[1], 10);
  const monRaw = m[2].toLowerCase().replace(/\./g, '');
  const year = parseInt(m[3], 10);
  const hourLocal = m[4] ? parseInt(m[4], 10) : 12; // default jam siang biar netral
  const minuteLocal = m[5] ? parseInt(m[5], 10) : 0;

  const map = (lang || '').toLowerCase() === 'id' ? MONTHS_ID : MONTHS_EN;
  const altMap = map === MONTHS_ID ? MONTHS_EN : MONTHS_ID;

  let month = map[monRaw];
  if (month == null) month = altMap[monRaw];

  if (month == null || !Number.isFinite(day) || !Number.isFinite(year)) return null;

  // Input dari situs = WIB (UTC+7) -> konversi ke UTC
  const WIB_OFFSET = 7; // jam
  const dtUtc = new Date(Date.UTC(year, month, day, hourLocal - WIB_OFFSET, minuteLocal, 0));

  return Number.isNaN(+dtUtc) ? null : dtUtc;
}




function buildNewsRow(n) {
  const pub =
    (n.publishedAt instanceof Date && !Number.isNaN(+n.publishedAt))
      ? n.publishedAt
      : parsePublishedAt(n.date, n.language || 'en'); // ← parse dari kolom "date"

  const row = {
    title: n.title,
    link: n.link,
    image: n.image,
    category: n.category,
    date: n.date,
    summary: n.summary,
    detail: n.detail || '',
    language: n.language || 'en',

    source_name: n.sourceName || 'Newsmaker23',
    source_url:  n.link,
    author:      n.author || null,
    author_name: n.author_name || toAuthorName(n.author),

    // 🔴 TIGA KOLOM YANG KAMU MAU SAMA DENGAN "date"
    published_at: pub || null,
    createdAt:    pub || null,
    updatedAt:    pub || null,
  };

  return row;
}




const delay = (ms) => new Promise((r) => setTimeout(r, ms));

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

const NEWS_ALLOWED_FIELDS = new Set([
  'id','title','link','image','category','date','summary','detail','language','createdAt',
  'source_name','source_url','author','author_name','published_at' // ← tambah author_name
]);
function normalizeFields(fields) {
  if (!fields) return null; // null = semua kolom
  const arr = fields.split(',').map(s => s.trim()).filter(Boolean);
  const picked = arr.filter(f => NEWS_ALLOWED_FIELDS.has(f));
  return picked.length ? picked : [
  'id','title','link','image','category','date','summary','detail','language','createdAt',
  'source_name','source_url','author','author_name','published_at' // ← tambah author_name
  ];
}

function normalizeSpace(s) {
  return (s || '').replace(/\s+/g, ' ').trim();
}

// ===== AUTHOR VALIDATION (refactor) =====

// 1) daftar inisial yang kita izinkan (whitelist) — huruf kecil semua
const AUTHOR_ALLOW = new Set([
  'cp','ayu','az','azf','yds','arl','alg','mrv'
]);

// 2) kata/akronim yang sering ke-detect tapi BUKAN author
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

// 3) alias salah-ketik → inisial valid (semua output huruf kecil)
const AUTHOR_ALIASES = {
  'cp':'cp', 'cP':'cp', 'CP':'cp',
  'ayu':'ayu','ayiu':'ayu','ayi':'ayu','ay':'ayu','ads':'ayu',
  'az':'az','azf':'azf',
  'yds':'yds',
  'arl':'arl',
  'alg':'alg',
  'mrv':'mrv'
};

// 4) peta inisial → nama lengkap (dipakai API)
const AUTHOR_NAME_MAP = {
  cp:  'Broto',
  ayu: 'Ayu',
  az:  'Nova',
  azf: 'Nova',
  yds: 'Yudis',
  arl: 'Arul',
  alg: 'Burhan',
  mrv: 'Marvy',
};

// normalisasi & validasi inisial
function normalizeAuthorInitial(raw) {
  if (!raw) return null;
  let s = String(raw).trim();

  // ambil pola paling umum: (xxx) atau teks polos "xxx"
  const paren = s.match(/\(([a-z]{2,6})\)/i);
  if (paren) s = paren[1];

  s = s.toLowerCase();

  // mapping typo → inisial
  if (AUTHOR_ALIASES[s]) s = AUTHOR_ALIASES[s];

  // hanya huruf a-z 2–6
  if (!/^[a-z]{2,6}$/.test(s)) return null;

  // drop jika di blacklist
  if (AUTHOR_BLACKLIST.has(s)) return null;

  // wajib terdaftar di whitelist
  if (!AUTHOR_ALLOW.has(s)) return null;

  return s;
}

function toAuthorName(initial) {
  const key = (initial || '').toLowerCase();
  return AUTHOR_NAME_MAP[key] || null;
}

// Ekstraksi dari HTML: cari (xxx) terdekat
function extractAuthorFromHtml(html = '') {
  const m = String(html).match(/\(([a-z]{2,6})\)/i);
  return normalizeAuthorInitial(m ? m[1] : null);
}

// Ekstraksi dari teks plain
function extractAuthorFromText(raw = '') {
  const m = String(raw).match(/\(([a-z]{2,6})\)/i);
  return normalizeAuthorInitial(m ? m[1] : null);
}

function sanitizeAuthor(raw) {
  if (!raw) return null;
  let s = String(raw).trim().toLowerCase();

  // ambil hanya huruf a-z, panjang 2–6 (gaya inisial newsroom)
  const m = s.match(/^[a-z]{2,6}$/i);
  if (!m) return null;
  s = m[0].toLowerCase();

  // mapping typo → alias
  if (AUTHOR_ALIASES[s]) s = AUTHOR_ALIASES[s];

  // buang kalau termasuk blacklist
  if (AUTHOR_BLACKLIST.has(s)) return null;

  return s;
}

function extractSourceNameFromHtml(html = '') {
  // dukung "Source:" dan "Sumber:"
  const m = String(html).match(/\b(?:Source|Sumber)\s*:?\s*([^<\n\r]+)/i);
  return m ? m[1].trim() : null;
}

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

  // fallback author dari summary (mis. "(ayu)") – di mana saja
  let author = extractAuthorFromText(summary);
  if (author) {
    summary = summary.replace(/\(([a-z]{2,8})\)/i, '').trim();
  }

  // published date wajib
  const publishedAt = parsePublishedAt(date, lang) || null;

  if (!title || !link) return null;
  return { title, link, image, category, date, summary, author: author || null, publishedAt };
}

async function retryRequest(fn, retries = 3, delayMs = 500) {
  try {
    return await fn();
  } catch (err) {
    if (retries === 0) throw err;
    await delay(delayMs);
    return retryRequest(fn, retries - 1, delayMs * 2);
  }
}
// Header ala browser supaya lolos WAF/CDN
const HTML_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
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
  return {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': isID ? 'id-ID,id;q=1,en;q=0.5' : 'en-US,en;q=1,id;q=0.5',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'DNT': '1',
  };
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

// Batching paralel terbatas (untuk fetch detail, dsb)
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

// Distributed lock biar job nggak dobel di Railway
async function withLock(lockKey, ttlSec, fn) {
  const ok = await redis.set(lockKey, Date.now(), 'NX', 'EX', ttlSec);
  if (!ok) {
    console.log(`🔒 Skip: lock ${lockKey} sedang aktif`);
    return;
  }
  try {
    await fn();
  } finally {
    try { await redis.del(lockKey); } catch { }
  }
}

// ====== YT SHORTS HELPERS ======
const ytCache = new NodeCache({ stdTTL: 300, checkperiod: 60 });

function ytBuildFeedUrl({ channelId, user, playlistId }) {
  if (channelId) return `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
  if (user) return `https://www.youtube.com/feeds/videos.xml?user=${user}`;
  if (playlistId) return `https://www.youtube.com/feeds/videos.xml?playlist_id=${playlistId}`;
  return null;
}

// Resolve @handle -> channelId by scraping channel page
async function ytResolveHandleToChannelId(handleRaw) {
  const handle = (handleRaw || '').startsWith('@') ? handleRaw.slice(1) : String(handleRaw || '').trim();
  if (!handle) throw new Error('Handle kosong');

  const url = `https://www.youtube.com/@${handle}`;
  const cacheKey = `yt:handle2cid:${handle}`;
  const memHit = ytCache.get(cacheKey);
  if (memHit) return memHit;

  const resp = await axios.get(url, {
    timeout: 15000,
    headers: HTML_HEADERS,
    maxRedirects: 2,
    responseType: 'text',
  });

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

// Parse RSS, dukung yt:duration@seconds atau media:content@duration
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

    const description = typeof mg['media:description'] === 'string'
      ? mg['media:description']
      : mg['media:description']?.['#text'] || '';

    const looksLikeShorts = /#shorts/i.test(`${title} ${description}`) || /\/shorts\//i.test(link);

    return {
      videoId,
      title,
      url: link,
      publishedAt: e.published,
      thumbnail: thumb,
      durationSeconds,
      looksLikeShorts,
    };
  }).filter((it) => {
    if (it.durationSeconds != null) return it.durationSeconds <= maxDuration;
    return guessShortsIfNoDuration ? it.looksLikeShorts : false;
  });

  return {
    meta: {
      title: feed?.title ?? null,
      author: feed?.author?.name ?? null,
      total: list.length,
    },
    items: list,
  };
}

// ====== NEWS ======
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

async function fetchNewsDetailSafe(url, lang = 'en') {
  return retryRequest(async () => {
    const { data } = await axios.get(url, {
      timeout: 180000,
      headers: makeHtmlHeaders(lang),
      maxRedirects: 3,
    });
    const $ = cheerio.load(data);
    if (isWafOrChallenge($)) {
      console.warn(`🛡️ WAF/Challenge terdeteksi saat ambil detail: ${url}`);
      return { text: '', author: null, sourceName: null };
    }

    const $root = $('div.article-content').first();
    const $article = $root.clone();

    // 1) Coba cari paragraf "Source:" / "Sumber:"
    let author = null;
    let sourceName = null;

    // 1) Cari paragraf "Source:" / "Sumber:" dulu
    const $sourceP = $article.find('p').filter((_, el) =>
      /\b(?:source|sumber)\b\s*:/i.test($(el).text())
    ).first();

    if ($sourceP.length) {
      const pHtml = $sourceP.html() || '';
      author = extractAuthorFromHtml(pHtml) || author;
      sourceName = extractSourceNameFromHtml(pHtml) || sourceName;
      $sourceP.remove();
    }

    // 2) Sapu bersih paragraf lain: ambil (xxx) PERTAMA yang valid
    if (!author) {
      $article.find('p').each((_, p) => {
        if (author) return;
        const a = extractAuthorFromHtml($(p).html() || '');
        if (a) author = a;
      });
    }

    // 3) Meta author (kalau ada & valid)
    if (!author) {
      const metaAuthor = $('meta[name="author"]').attr('content');
      const a = sanitizeAuthor(metaAuthor);
      if (a) author = a;
    }


    // Bersihkan elemen tak relevan & trailing (xxx)
    $article.find('p').each((_, p) => {
      const html = $(p).html() || '';
      const cleaned = html
        // hapus penanda inisial di akhir paragraf
        .replace(/\s*\([a-z]{2,8}\)\s*$/i, '')
        // juga jika ada di tengah kalimat tapi berdiri sendiri (opsional)
        .replace(/\s*\([a-z]{2,8}\)\s*(?=[\.\,\;\:])/gi, '');
      if (cleaned !== html) $(p).html(cleaned);
    });

    // ✅ Ambil semua paragraf, kasih \n\n supaya layout tetap ada
    const paragraphs = [];
    $article.find('p').each((_, p) => {
      const txt = normalizeSpace($(p).text());
      if (txt) paragraphs.push(txt);
    });
    const plainText = paragraphs.join("\n\n"); // <-- ini bikin paragraf tetap ada

    return { text: plainText, author, sourceName };
  }, 3, 1000);
}

function looksIndonesian(s = '') {
  const t = (s || '').toLowerCase();
  const hits = (t.match(/\b(dan|yang|akan|dari|pada|dengan|sebagai)\b/g) || []).length;
  const anti = (t.match(/\b(the|and|of|for|with|as|to)\b/g) || []).length;
  return hits >= anti;
}

const MAX_PAGES_PER_CAT = 200;
const PAGE_SIZE = 10;
const MAX_PAGE_EMPTY_STREAK = 3;

// Core: scrape per bahasa (anti-duplikat + no-skip + pagination aman)
async function scrapeNewsByLang(lang = 'en') {
  console.log(`🚀 Scraping news (${lang})...`);

  const { Op } = require('sequelize');
  const existing = await News.findAll({
    where: { language: lang },
    attributes: ['link'],
    raw: true,
  });
  const existingLinks = new Set(existing.map((r) => r.link));
  const seenLinks = new Set();
  const allNewItems = [];

  for (const cat of newsCategories) {
    let start = 0;
    let emptyStreak = 0;

    while (true) {
      const url = `https://www.newsmaker.id/index.php/${lang}/${cat}?start=${start}`;
      try {
        const { data } = await retryRequest(
          () => axios.get(url, { timeout: 180000, headers: makeHtmlHeaders(lang), maxRedirects: 3 }),
          3,
          1000
        );

        const $ = cheerio.load(data);
        if (isWafOrChallenge($)) {
          console.warn(`🛡️ Blocked page: ${url}`);
          emptyStreak++;
          if (emptyStreak >= 3) break;
          start += PAGE_SIZE;
          continue;
        }

        const items = [];
        $('div.single-news-item').each((_, el) => {
          const item = extractNewsItem($, el, lang);
          if (item) items.push(item);
        });

        const fresh = items.filter(
          (it) => !existingLinks.has(it.link) && !seenLinks.has(it.link)
        );
        fresh.forEach((it) => seenLinks.add(it.link));

        if (fresh.length === 0) {
          emptyStreak++;
          if (emptyStreak >= 3) break;
        } else {
          emptyStreak = 0;

          const detailTasks = fresh.map((it) => async () => {
            const detail = await fetchNewsDetailSafe(it.link, lang);

            const sourceName = detail?.sourceName || 'Newsmaker23';
let author = detail?.author
  || extractAuthorFromText(it.summary || '')
  || it.author
  || null;

author = normalizeAuthorInitial(author); // ← pakai normalizer baru
            if (author) console.log(`✍️ author (${author}) -> ${it.link}`);
            if (detail?.sourceName) console.log(`🔗 source "${detail.sourceName}" -> ${it.link}`);

            return {
              ...it,
              detail: detail?.text || '',
              author,
              author_name: toAuthorName(author), // ✅ tambahkan ini
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
        console.warn(`⚠️ Gagal ambil halaman: ${url} | ${e.message}`);
        emptyStreak++;
        if (emptyStreak >= 3) break;
        start += PAGE_SIZE;
        await delay(300);
      }
    }
  }

  if (allNewItems.length > 0) {
    const rows = allNewItems.map(buildNewsRow);

    // 🔁 Penting: upsert agar row lama (author null) ikut ter-update
 // --- batched bulkCreate + hard cap detail size ---
const UPDATE_COLS = [
  'summary','detail','author','author_name','source_name','source_url',
  'published_at','image','category','date','language','title',
  'createdAt','updatedAt' // ← tambah ini
];


 const MAX_DETAIL_CHARS = 500_000; // ~0.5 MB per row, silakan sesuaikan
 const BATCH_SIZE = 150;            // 100–200 aman

 // trimming dulu biar payload lebih ringan
 const trimmed = rows.map(r => ({
   ...r,
   detail: (r.detail || '').slice(0, MAX_DETAIL_CHARS),
 }));

 for (let i = 0; i < trimmed.length; i += BATCH_SIZE) {
   const chunk = trimmed.slice(i, i + BATCH_SIZE);
   await News.bulkCreate(chunk, {
     updateOnDuplicate: UPDATE_COLS,
     logging: false,
   });
  }
     }
}

// Wrapper untuk dipakai scheduler/endpoint
async function scrapeNews() {
  return withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en'));
}
async function scrapeNewsID() {
  return withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id'));
}

// ===== Calendar =====
async function scrapeCalendar() {
  console.log('Scraping calendar with Puppeteer...');
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    const page = await browser.newPage();
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0.0.0 Safari/537.36'
    );

    await page.goto('https://www.newsmaker.id/index.php/en/analysis/economic-calendar', {
      waitUntil: 'networkidle2',
      timeout: 60000,
    });

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

        if (!time || !currency || !impact || !raw || raw === '-' || currency === '-' || raw.includes('2025-')) {
          continue;
        }

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
    console.log(`✅ Calendar updated (${cachedCalendar.length} valid events)`);

    try {
      await redis.set(
        'calendar:all',
        JSON.stringify({
          status: 'success',
          updatedAt: lastUpdatedCalendar,
          total: cachedCalendar.length,
          data: cachedCalendar,
        }),
        'EX',
        60 * 15
      );
      console.log('🧠 Calendar data saved to Redis with TTL 15 minutes');
    } catch (err) {
      console.error('❌ Failed to save calendar to Redis:', err.message);
    }
  } catch (err) {
    if (browser) await browser.close();
    console.error('❌ Calendar scraping failed:', err.message);
  }
}

// ===== Historical =====
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
  if (cachedSymbols && now - cachedSymbolsTimestamp) {
    console.log(`🟡 Using cached symbols: ${cachedSymbols.length}`);
    return cachedSymbols;
  }
  const { data } = await axios.get(BASE_URL, { headers: HTML_HEADERS });
  const $ = cheerio.load(data);
  const options = $('select[name="cid"] option');
  const symbols = [];
  options.each((_, el) => {
    const cid = $(el).attr('value');
    const name = $(el).text().trim();
    if (cid && name) symbols.push({ cid, name });
  });
  console.log(`✅ Fetched ${symbols.length} symbols from base URL`);
  cachedSymbols = symbols;
  cachedSymbolsTimestamp = now;
  return symbols;
}

async function scrapePageForSymbol(cid, start, retries = 3, backoff = 1000) {
  try {
    const url = `${BASE_URL}?cid=${cid}&period=d&start=${start}`;
    console.log(`📄 Scraping page for cid=${cid} start=${start}`);
    const { data } = await axios.get(url, {
      timeout: 120000,
      headers: HTML_HEADERS,
      maxRedirects: 3,
    });

    const $ = cheerio.load(data);
    const table = $('table.table.table-striped.table-bordered');
    if (table.length === 0) {
      console.warn(`⚠️ No table found for cid=${cid} start=${start}`);
      return [];
    }

    const rows = table.find('tbody tr');
    const result = [];

    rows.each((i, row) => {
      const $row = $(row);
      const cols = $row.find('td');

      let hasColspan = false;
      cols.each((_, col) => {
        if ($(col).attr('colspan')) {
          hasColspan = true;
          return false;
        }
      });

      if (hasColspan) {
        const date = cols.first().text().trim();
        const event = cols.last().text().trim();
        result.push({
          date,
          event,
          open: null,
          high: null,
          low: null,
          close: null,
          change: null,
          volume: null,
          openInterest: null,
        });
        return;
      }

      const textCols = cols.map((_, el) => $(el).text().trim()).get();
      if (textCols.length === 0) return;

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

      if (
        rowData.date &&
        (rowData.open !== null ||
          rowData.close !== null ||
          rowData.high !== null ||
          rowData.low !== null)
      ) {
        result.push(rowData);
      }
    });

    console.log(`✅ Scraped ${result.length} rows for cid=${cid} start=${start}`);
    return result;
  } catch (err) {
    if (retries > 0) {
      console.warn(
        `⏳ Retry scraping cid=${cid} start=${start}, left: ${retries}, error: ${err.message}`
      );
      await delay(backoff);
      return scrapePageForSymbol(cid, start, retries - 1, backoff * 2);
    } else {
      console.error(`❌ Failed to scrape cid=${cid} start=${start}:`, err.message);
      return [];
    }
  }
}

async function scrapeAllDataForSymbol(cid, maxRows = 5000) {
  console.log(`🎯 Starting complete scrape for cid=${cid} (max ${maxRows} rows)`);
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
      consecutiveEmptyPages++;
      console.log(`📭 Empty page ${consecutiveEmptyPages} for cid=${cid} at start=${start}`);
      if (consecutiveEmptyPages >= 3) break;
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

  console.log(`🎉 Completed scraping cid=${cid}: ${allData.length} total rows`);
  return allData;
}

async function scrapeAllHistoricalData() {
  console.log('📊 Scraping all historical data...');
  const symbols = await getAllSymbols();
  console.log(`🔎 Total symbols to scrape: ${symbols.length}`);
  const executing = new Set();

  async function runWithLimit(task) {
    while (executing.size >= MAX_CONCURRENT_SCRAPES) {
      await Promise.race(executing);
    }
    const p = task();
    executing.add(p);
    p.finally(() => executing.delete(p));
    return p;
  }

  await Promise.all(
    symbols.map(({ cid, name }) =>
      runWithLimit(async () => {
        console.log(`🚀 Scraping ${name} (cid=${cid})...`);
        const data = await scrapeAllDataForSymbol(cid);
        console.log(`✅ ${name}: ${data.length} rows`);

        for (const row of data) {
          try {
            const [record, created] = await HistoricalData.findOrCreate({
              where: { symbol: name, date: row.date },
              defaults: {
                event: row.event || null,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                change: row.change,
                volume: row.volume,
                openInterest: row.openInterest,
              },
            });

            if (created) {
              console.log(`✅ Saved: ${name} (${row.date})`);
            } else {
              console.log(`⏭️ Skipped (exists): ${name} (${row.date})`);
            }
          } catch (err) {
            console.error(`❌ Failed to save row for ${name} (${row.date}): ${err.message}`);
          }
        }

        const cacheKey = `historical:${name.toLowerCase()}:all`;
        try {
          await redis.set(
            cacheKey,
            JSON.stringify({ status: 'success', symbol: name, data, updatedAt: new Date() }),
            'EX',
            60 * 60 * 2
          );
          console.log(`💾 Cached historical data for ${name} in Redis`);
        } catch (err) {
          console.error(`❌ Redis cache error for ${name}:`, err.message);
        }
      })
    )
  );

  console.log('🎉 All scraping completed.');
  return true;
}

// === Fallback author EN -> ambil dari artikel ID yang sama ===
async function fillAuthorFromIDIfMissing(data) {
  // data bisa berupa instance Sequelize atau plain object
  const row = data.toJSON ? data.toJSON() : { ...data };

  // hanya kalau EN dan masih kosong
  if ((row.language || '').toLowerCase() !== 'en') return row;
  if (row.author || row.author_name) return row;

  const { Op } = require('sequelize');

  // 1) coba pasangannya via link (/en/ -> /id/)
  let altLink = null;
  if (row.link) {
    altLink = row.link
      .replace('/index.php/en/', '/index.php/id/')
      .replace('/en/', '/id/');
  }

  let match = null;

  if (altLink) {
    match = await News.findOne({
      where: { language: 'id', link: altLink },
      attributes: ['author', 'author_name'],
      order: [['published_at', 'DESC']],
      logging: false,
    });
  }

  // 2) kalau belum ketemu, coba cocokkan via image (sering sama)
  if (!match && row.image) {
    match = await News.findOne({
      where: { language: 'id', image: row.image },
      attributes: ['author', 'author_name'],
      order: [['published_at', 'DESC']],
      logging: false,
    });
  }

  // 3) terakhir, jendela waktu ±6 jam + kategori & (opsional) sumber
  if (!match && row.published_at) {
    const t = new Date(row.published_at);
    const t1 = new Date(t.getTime() - 6 * 60 * 60 * 1000);
    const t2 = new Date(t.getTime() + 6 * 60 * 60 * 1000);

    const where = {
      language: 'id',
      published_at: { [Op.between]: [t1, t2] },
    };
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

// ===== Schedulers (pakai lock) =====
withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en'));
withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id'));
// scrapeCalendar();
// withLock('lock:hist:all', 3600, () => scrapeAllHistoricalData());

// setInterval(() => withLock('lock:hist:all', 3600, () => scrapeAllHistoricalData()), 4 * 60 * 60 * 1000); // tiap 4 jam
setInterval(() => withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en')), 10 * 60 * 1000); // 10 menit
setInterval(() => withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id')), 10 * 60 * 1000); // 10 menit
setInterval(scrapeCalendar, 60 * 60 * 1000); // 1 jam

// ===== API =====
// ===== NEWS (EN) - paginated, fields, Redis cache, ETag =====
app.get('/api/news', async (req, res) => {
  try {
    const { category = 'all', search = '', page = '1', limit = '500', fields = '' } = req.query;

    const p = Math.max(parseInt(page, 10) || 1, 1);
    const l = Math.min(Math.max(parseInt(limit, 10) || 20, 1), 500);
    const attrs = normalizeFields(fields);

    const { Op } = require('sequelize');
    const where = { language: 'en' };
    if (category !== 'all') where.category = { [Op.like]: `%${category}%` };
    if (search) {
      where[Op.or] = [
        { title: { [Op.like]: `%${search}%` } },
        { summary: { [Op.like]: `%${search}%` } },
        { detail: { [Op.like]: `%${search}%` } },
      ];
    }

    const cacheKey = makeNewsCacheKey({
      lang: 'en', category, search, page: p, limit: l, fields: attrs?.join(',')
    });
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 30);

    // pastikan author_name ada di payload
const { rows, count } = await News.findAndCountAll({
  where,
  attributes: attrs || undefined,
  order: [
    ['published_at', 'DESC'],
    ['createdAt', 'DESC'],
  ],
  limit: l,
  offset: (p - 1) * l,
});

const data = rows.map(r => {
  const row = r.toJSON ? r.toJSON() : r;
  if (!row.author_name) row.author_name = toAuthorName(row.author) || null; // ← cek null/empty, bukan "in"
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

// ===== NEWS (ID) - paginated, fields, Redis cache, ETag =====
app.get('/api/news-id', async (req, res) => {
  try {
    const { category = 'all', search = '', page = '1', limit = '500', fields = '' } = req.query;

    const p = Math.max(parseInt(page, 10) || 1, 1);
    const l = Math.min(Math.max(parseInt(limit, 10) || 20, 1), 500);
    const attrs = normalizeFields(fields);

    const { Op } = require('sequelize');
    const where = { language: 'id' };
    if (category !== 'all') where.category = { [Op.like]: `%${category}%` };
    if (search) {
      where[Op.or] = [
        { title: { [Op.like]: `%${search}%` } },
        { summary: { [Op.like]: `%${search}%` } },
        { detail: { [Op.like]: `%${search}%` } },
      ];
    }

    const cacheKey = makeNewsCacheKey({
      lang: 'id', category, search, page: p, limit: l, fields: attrs?.join(',')
    });
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 30);

    const { rows, count } = await News.findAndCountAll({
      where,
      attributes: attrs || undefined,
      order: [
        ['published_at', 'DESC'],
        ['createdAt', 'DESC'],
      ],
      limit: l,
      offset: (p - 1) * l,
    });

const data = rows.map(r => {
  const row = r.toJSON ? r.toJSON() : r;
  if (!row.author_name) row.author_name = toAuthorName(row.author) || null; // ← cek null/empty, bukan "in"
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

// ===== NEWS DETAIL by ID =====
app.get('/api/news/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });

    const cacheKey = `news:item:${id}`;
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 120);

    const row = await News.findByPk(id);
    if (!row) return res.status(404).json({ error: 'Not found' });

    // pastikan author_name minimal fallback dari inisial
    let data = row.toJSON ? row.toJSON() : row;
    if (!data.author_name) data.author_name = toAuthorName(data.author) || null;

    // 👇 kalau EN dan masih kosong → ambil dari versi ID
    data = await fillAuthorFromIDIfMissing(data);

    const payload = { status: 'success', data };
    await redis.set(cacheKey, JSON.stringify(payload), 'EX', 120);

    return sendWithETag(req, res, payload, 120);
  } catch (err) {
    console.error('❌ /api/news/:id error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});


app.get('/api/news-id/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });

    const cacheKey = `news:item:${id}`;
    const cached = await redis.get(cacheKey);
    if (cached) return sendWithETag(req, res, JSON.parse(cached), 120);

    const row = await News.findByPk(id);
    if (!row) return res.status(404).json({ error: 'Not found' });

    // 👇 sama: fallback author_name
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


// ===== Calendar API =====
app.get('/api/calendar', async (req, res) => {
  try {
    const cached = await redis.get('calendar:all');
    if (cached) {
      console.log('📦 Serving calendar from Redis cache');
      return res.json(JSON.parse(cached));
    }

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
    console.error('❌ Error in /api/calendar:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// /api/historical — MySQL direct, sorted per-symbol by latest date (date = VARCHAR)
app.get('/api/historical', async (req, res) => {
  try {
    const { QueryTypes } = require('sequelize');
    const tableName = HistoricalData.getTableName().toString();

    const orderedSymbols = await sequelize.query(
      `
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
      `,
      { type: QueryTypes.SELECT }
    );

    if (!orderedSymbols.length) {
      return res.status(404).json({
        status: 'empty',
        message: 'No historical data found in database.',
      });
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

      allData.push({
        symbol,
        data: rows,
        updatedAt: row.updatedAtMax || null,
      });
    }

    return res.json({
      status: 'success',
      totalSymbols: allData.length,
      data: allData,
    });
  } catch (err) {
    console.error('❌ Error in /api/historical (MySQL direct, varchar date):', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ===== REFACTORED: QUOTES NO-CACHE =====
app.get('/api/quotes', async (req, res) => {
  try {
    const url =
      'https://www.newsmaker.id/quotes/live?s=LGD+LSI+GHSIU5+LCOPX5+SN1U5+DJIA+DAX+DX+AUDUSD+EURUSD+GBPUSD+CHF+JPY+RP';

    const { data } = await axios.get(url, { timeout: 15000 });

    const quotes = [];
    for (let i = 1; i <= data[0].count; i++) {
      const high = data[i].high !== 0 ? data[i].high : data[i].last;
      const low = data[i].low !== 0 ? data[i].low : data[i].last;
      const open = data[i].open !== 0 ? data[i].open : data[i].last;

      quotes.push({
        symbol: data[i].symbol,
        last: data[i].last,
        high,
        low,
        open,
        prevClose: data[i].prevClose,
        valueChange: data[i].valueChange,
        percentChange: data[i].percentChange,
      });
    }

    return res.json({
      status: 'success',
      updatedAt: new Date(),
      total: quotes.length,
      data: quotes,
      source: 'live',
    });
  } catch (err) {
    console.error('❌ Live quotes fetch failed:', err.message);

    if (Array.isArray(cachedQuotes) && cachedQuotes.length > 0) {
      const validQuotes = cachedQuotes.map((q) => ({
        ...q,
        high: q.high !== 0 ? q.high : q.last,
        low: q.low !== 0 ? q.low : q.last,
        open: q.open !== 0 ? q.open : q.last,
      }));
      return res.json({
        status: 'degraded',
        updatedAt: lastUpdatedQuotes,
        total: validQuotes.length,
        data: validQuotes,
        source: 'fallback-cache',
      });
    }

    return res.status(502).json({
      status: 'error',
      message: 'Failed to fetch live quotes',
      detail: err.message,
    });
  }
});

// ===== Shorts =====
app.get('/api/shorts', async (req, res) => {
  try {
    let { handle, channelId, user, playlistId } = req.query;
    const limit = Math.max(parseInt(req.query.limit, 10) || 20, 1);
    const maxDuration = Math.max(parseInt(req.query.maxDuration, 10) || 61, 1);
    const guess = String(req.query.guess || '1') === '1';
    const cacheTtl = parseInt(req.query.cacheTtl, 10) || 300;

    if (!handle && !channelId && !user && !playlistId) {
      return res.status(400).json({
        ok: false,
        error: 'Sertakan salah satu: handle / channelId / user / playlistId',
        example: '/api/shorts?handle=NewsMaker23&limit=15&guess=1',
      });
    }

    // Resolve handle -> channelId jika perlu
    if (handle && !channelId) {
      channelId = await ytResolveHandleToChannelId(handle);
    }

    const feedUrl = ytBuildFeedUrl({ channelId, user, playlistId });
    if (!feedUrl) return res.status(400).json({ ok: false, error: 'Parameter tidak valid' });

    const rKey = `yt:shorts:${feedUrl}:max${maxDuration}:guess${guess ? 1 : 0}`;
    const memHit = ytCache.get(rKey);
    if (memHit) {
      return sendWithETag(req, res, {
        ok: true,
        source: 'cache-mem',
        feedUrl,
        count: Math.min(memHit.items.length, limit),
        meta: memHit.meta,
        data: memHit.items.slice(0, limit),
      }, 60);
    }

    // Coba Redis
    try {
      const r = await redis.get(rKey);
      if (r) {
        const parsed = JSON.parse(r);
        ytCache.set(rKey, parsed, cacheTtl); // hydrate RAM
        return sendWithETag(req, res, {
          ok: true,
          source: 'cache-redis',
          feedUrl,
          count: Math.min(parsed.items.length, limit),
          meta: parsed.meta,
          data: parsed.items.slice(0, limit),
        }, 60);
      }
    } catch (e) {
      console.warn('⚠️ Redis get error (shorts):', e.message);
    }

    // Fetch fresh
    const { data: xml } = await axios.get(feedUrl, { timeout: 15000, responseType: 'text' });
    const parsed = ytExtractShorts(xml, { maxDuration, guessShortsIfNoDuration: guess });

    // Simpan cache
    ytCache.set(rKey, parsed, cacheTtl);
    try {
      await redis.set(rKey, JSON.stringify(parsed), 'EX', cacheTtl);
    } catch (e) {
      console.warn('⚠️ Redis set error (shorts):', e.message);
    }

    return sendWithETag(req, res, {
      ok: true,
      source: 'live',
      feedUrl,
      count: Math.min(parsed.items.length, limit),
      meta: parsed.meta,
      data: parsed.items.slice(0, limit),
    }, 60);
  } catch (err) {
    console.error('❌ /api/shorts error:', err.message);
    res.status(500).json({ ok: false, error: 'Internal server error', detail: err.message });
  }
});

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
    lastUpdated: {
      news: lastUpdatedNews,
      newsID: lastUpdatedNewsID,
      calendar: lastUpdatedCalendar,
      quotes: lastUpdatedQuotes,
    },
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Server ready at http://localhost:${PORT}`);
});