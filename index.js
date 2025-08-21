require('dotenv').config();
const express = require('express');
const cors = require('cors');
const puppeteer = require('puppeteer');
const axios = require('axios');
const cheerio = require('cheerio');
const { sequelize, News, HistoricalData } = require('./models');
const Redis = require('ioredis');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());

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
const delay = (ms) => new Promise((r) => setTimeout(r, ms));

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

function normalizeSpace(s) {
  return (s || '').replace(/\s+/g, ' ').trim();
}

function extractNewsItem($, el) {
  const $el = $(el);
  const title = normalizeSpace($el.find('h5.card-title a').text());
  const href = $el.find('h5.card-title a').attr('href');
  const link = href ? 'https://www.newsmaker.id' + href : null;
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

  // jangan skip hanya karena summary kosong; minimal title & link
  if (!title || !link) return null;
  return { title, link, image, category, date, summary };
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
    try { await redis.del(lockKey); } catch {}
  }
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

async function fetchNewsDetailSafe(url) {
  return retryRequest(async () => {
    const { data } = await axios.get(url, {
      timeout: 180000, // 3 menit
      headers: HTML_HEADERS,
      maxRedirects: 3,
    });
    const $ = cheerio.load(data);
    if (isWafOrChallenge($)) {
      console.warn(`🛡️ WAF/Challenge terdeteksi saat ambil detail: ${url}`);
      return { text: '' };
    }
    const articleDiv = $('div.article-content').clone();
    articleDiv.find('span, h3').remove();
    const plainText = articleDiv.text().trim();
    return { text: plainText };
  }, 3, 1000);
}

const MAX_PAGES_PER_CAT = 200;
const PAGE_SIZE = 10;
const MAX_PAGE_EMPTY_STREAK = 3;

// Core: scrape per bahasa (anti-duplikat + no-skip + pagination aman)
async function scrapeNewsByLang(lang = 'en') {
  console.log(`🚀 Scraping news (${lang}) with de-dup & no-skip...`);

  // Ambil link yang sudah ada di DB untuk bahasa ini
  const { Op } = require('sequelize');
  const existing = await News.findAll({
    where: { language: lang },
    attributes: ['link'],
    raw: true,
  });
  const existingLinks = new Set(existing.map((r) => r.link));
  const seenLinks = new Set(); // de-dup di sesi ini
  const allNewItems = [];

  for (const cat of newsCategories) {
    let start = 0;
    let emptyStreak = 0;
    let pageCount = 0;

    while (pageCount < MAX_PAGES_PER_CAT) {
      const url = `https://www.newsmaker.id/index.php/${lang}/${cat}?start=${start}`;
      try {
        const { data } = await retryRequest(
          () =>
            axios.get(url, {
              timeout: 180000,
              headers: HTML_HEADERS,
              maxRedirects: 3,
            }),
          3,
          1000
        );

        const $ = cheerio.load(data);
        if (isWafOrChallenge($)) {
          console.warn(`🛡️ WAF/Challenge terdeteksi di list page: ${url}`);
          // anggap kosong agar tidak infinite loop
          emptyStreak++;
          if (emptyStreak >= MAX_PAGE_EMPTY_STREAK) break;
          start += PAGE_SIZE;
          pageCount++;
          await delay(200);
          continue;
        }

        const items = [];
        $('div.single-news-item').each((_, el) => {
          const item = extractNewsItem($, el);
          if (item) items.push(item);
        });

        const foundCount = items.length;
        const wouldBeInDb = items.filter((it) => existingLinks.has(it.link)).length;
        const wouldBeDup = items.filter((it) => seenLinks.has(it.link)).length;

        // filter fresh
        const fresh = items.filter(
          (it) => !existingLinks.has(it.link) && !seenLinks.has(it.link)
        );
        fresh.forEach((it) => seenLinks.add(it.link));

        console.log(
          `${cat} [${lang}] start=${start} → found=${foundCount}, inDB=${wouldBeInDb}, dupSession=${wouldBeDup}, fresh=${fresh.length}`
        );

        if (fresh.length === 0) {
          emptyStreak++;
          if (emptyStreak >= MAX_PAGE_EMPTY_STREAK) break;
        } else {
          emptyStreak = 0;
          // ambil detail dgn paralel terbatas
          const detailTasks = fresh.map((it) => async () => {
            const detail = await fetchNewsDetailSafe(it.link);
            return { ...it, detail: detail?.text || '' };
          });
          const detailed = (await runParallelWithLimit(detailTasks, 4)).filter(Boolean);
          allNewItems.push(...detailed);
        }

        start += PAGE_SIZE;
        pageCount++;
        await delay(120); // santun
      } catch (e) {
        console.warn(`⚠️ Gagal ambil halaman: ${url} | ${e.message}`);
        emptyStreak++;
        if (emptyStreak >= MAX_PAGE_EMPTY_STREAK) break;
        start += PAGE_SIZE;
        pageCount++;
        await delay(300);
      }
    }
  }

  if (allNewItems.length > 0) {
    try {
      await News.bulkCreate(
        allNewItems.map((n) => ({
          title: n.title,
          link: n.link,
          image: n.image,
          category: n.category,
          date: n.date,
          summary: n.summary,
          detail: n.detail || '',
          language: lang,
        })),
        { ignoreDuplicates: true } // efektif jika ada unique index (link) atau (link, language)
      );
      console.log(`✅ [${lang}] Saved to DB: ${allNewItems.length} rows (ignoreDuplicates)`);
    } catch (err) {
      console.error(`❌ [${lang}] bulkCreate failed: ${err.message}`);
      // fallback upsert per item
      for (const n of allNewItems) {
        try {
          await News.upsert({
            title: n.title,
            link: n.link,
            image: n.image,
            category: n.category,
            date: n.date,
            summary: n.summary,
            detail: n.detail || '',
            language: lang,
          });
        } catch (e) {
          console.error(`❌ upsert gagal untuk ${n.link}: ${e.message}`);
        }
      }
    }
  }

  if (lang === 'en') {
    cachedNews = allNewItems;
    lastUpdatedNews = new Date();
    const keys = await redis.keys('news:*');
    if (keys.length) await redis.del(...keys);
    console.log(`✅ News EN updated (${cachedNews.length} items)`);
  } else {
    cachedNewsID = allNewItems;
    lastUpdatedNewsID = new Date();
    const keys = await redis.keys('newsID:*');
    if (keys.length) await redis.del(...keys);
    console.log(`✅ News ID updated (${cachedNewsID.length} items)`);
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

// ===== Live Quotes =====
// async function scrapeQuotes() {
//   console.log('Scraping quotes from JSON endpoint...');
//   const url =
//     'https://www.newsmaker.id/quotes/live?s=LGD+LSI+GHSIQ5+LCOPV5+SN1U5+DJIA+DAX+DX+AUDUSD+EURUSD+GBPUSD+CHF+JPY+RP';
//   try {
//     const { data } = await axios.get(url);
//     const quotes = [];
//     for (let i = 1; i <= data[0].count; i++) {
//       let high = data[i].high !== 0 ? data[i].high : data[i].last;
//       let low = data[i].low !== 0 ? data[i].low : data[i].last;
//       let open = data[i].open !== 0 ? data[i].open : data[i].last;

//       quotes.push({
//         symbol: data[i].symbol,
//         last: data[i].last,
//         high,
//         low,
//         open,
//         prevClose: data[i].prevClose,
//         valueChange: data[i].valueChange,
//         percentChange: data[i].percentChange,
//       });
//     }
//     cachedQuotes = quotes;
//     lastUpdatedQuotes = new Date();
//     console.log(`✅ Quotes updated (${quotes.length} items)`);
//   } catch (err) {
//     console.error('❌ Quotes scraping failed:', err.message);
//   }
// }

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

// ===== Schedulers (pakai lock) =====
withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en'));
withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id'));
scrapeCalendar();
// scrapeQuotes();
withLock('lock:hist:all', 3600, () => scrapeAllHistoricalData());

// // Note: 60*60*1000 = 1 jam
setInterval(() => withLock('lock:hist:all', 3600, () => scrapeAllHistoricalData()), 4 * 60 * 60 * 1000); // tiap 4 jam
setInterval(() => withLock('lock:scrapeNews:en', 300, () => scrapeNewsByLang('en')), 10 * 60 * 1000); // 10 menit
setInterval(() => withLock('lock:scrapeNews:id', 300, () => scrapeNewsByLang('id')), 10 * 60 * 1000); // 10 menit
setInterval(scrapeCalendar, 60 * 60 * 1000); // 1 jam
// setInterval(scrapeQuotes, 0.15 * 60 * 1000); // 9 detik

// ===== API =====
app.get('/api/news', async (req, res) => {
  const { category = 'all', search = '' } = req.query;
  const { Op } = require('sequelize');

  try {
    const where = { language: 'en' };
    if (category !== 'all') where.category = { [Op.like]: `%${category}%` };
    if (search) {
      where[Op.or] = [
        { title: { [Op.like]: `%${search}%` } },
        { summary: { [Op.like]: `%${search}%` } },
        { detail: { [Op.like]: `%${search}%` } },
      ];
    }

    const results = await News.findAll({ where, order: [['createdAt', 'DESC']] });

    res.json({ status: 'success', total: results.length, data: results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/news-id', async (req, res) => {
  const { category = 'all', search = '' } = req.query;
  const { Op } = require('sequelize');

  try {
    const where = { language: 'id' };
    if (category !== 'all') where.category = { [Op.like]: `%${category}%` };
    if (search) {
      where[Op.or] = [
        { title: { [Op.like]: `%${search}%` } },
        { summary: { [Op.like]: `%${search}%` } },
        { detail: { [Op.like]: `%${search}%` } },
      ];
    }

    const results = await News.findAll({ where, order: [['createdAt', 'DESC']] });

    res.json({ status: 'success', total: results.length, data: results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

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

    // 1) Urutkan daftar symbol berdasarkan tanggal terbaru masing-masing
    const orderedSymbols = await sequelize.query(
      `
      SELECT
        symbol,
        MAX(STR_TO_DATE(\`date\`, '%d %b %Y')) AS latestDate,
        MAX(updatedAt) AS updatedAtMax
      FROM \`${tableName}\`
      GROUP BY symbol
      ORDER BY latestDate IS NULL, latestDate DESC, updatedAtMax DESC
      `,
      { type: QueryTypes.SELECT }
    );

    if (!orderedSymbols.length) {
      return res.status(404).json({
        status: 'empty',
        message: 'No historical data found in database.',
      });
    }

    // 2) Ambil data per symbol (urut dari tanggal paling baru)
    const allData = [];
    for (const row of orderedSymbols) {
      const symbol = row.symbol;

      const rows = await HistoricalData.findAll({
        where: { symbol },
        order: [[sequelize.literal("STR_TO_DATE(`date`, '%d %b %Y')"), 'DESC']],
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
      'https://www.newsmaker.id/quotes/live?s=LGD+LSI+GHSIQ5+LCOPV5+SN1U5+DJIA+DAX+DX+AUDUSD+EURUSD+GBPUSD+CHF+JPY+RP';

    // Always fetch fresh from source (no cache usage)
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
      source: 'live', // penanda live fetch
    });
  } catch (err) {
    console.error('❌ Live quotes fetch failed:', err.message);

    // --- OPSIONAL: Fallback ke cache RAM kalau ada (boleh dihapus kalau mau pure no-cache) ---
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
    // -----------------------------------------------------------------------------------------

    return res.status(502).json({
      status: 'error',
      message: 'Failed to fetch live quotes',
      detail: err.message,
    });
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
