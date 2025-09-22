// models/news.js
const crypto = require('crypto');

/** @type {(sequelize: import('sequelize').Sequelize, DataTypes: typeof import('sequelize').DataTypes) => any} */
module.exports = (sequelize, DataTypes) => {
  const News = sequelize.define('News', {
    // ===== konten utama =====
    title:       { type: DataTypes.TEXT, allowNull: false, validate: { notEmpty: true } },
    link:        { type: DataTypes.STRING(512), allowNull: false, validate: { notEmpty: true, len: [1, 512] } },
    image:       { type: DataTypes.TEXT, allowNull: true },
    category:    { type: DataTypes.STRING(191), allowNull: true },
    date:        { type: DataTypes.STRING(191), allowNull: true },             // raw (legacy)
    summary:     { type: DataTypes.TEXT, allowNull: true },
    detail:      { type: DataTypes.TEXT('long'), allowNull: true },
    language:    { type: DataTypes.STRING(5), allowNull: false, validate: { isIn: [['en','id']] } },

    // ===== metadata Play Console =====
    source_name: { type: DataTypes.STRING(191), allowNull: false, defaultValue: 'Newsmaker 23' },
    source_url:  { type: DataTypes.TEXT,       allowNull: false },
    author:      { type: DataTypes.STRING(32), allowNull: true },
    author_name: { type: DataTypes.STRING(64), allowNull: true },
    published_at:{ type: DataTypes.DATE,       allowNull: false, defaultValue: DataTypes.NOW },

    // ===== push notification =====
    push_state: {
      // pending → siap kirim; sent → sukses; failed → gagal; skipped → diputuskan tidak dikirim
      type: DataTypes.ENUM('pending', 'sent', 'failed', 'skipped'),
      allowNull: false,
      defaultValue: 'pending',
    },
    push_sent_at:      { type: DataTypes.DATE, allowNull: true },
    push_error:        { type: DataTypes.TEXT, allowNull: true },
    push_topic:        { type: DataTypes.STRING(64), allowNull: true },        // mis. news_all / news_economy
    push_collapse_key: { type: DataTypes.STRING(64), allowNull: true },        // mis. news_123
    push_deeplink:     { type: DataTypes.STRING(512), allowNull: true },       // mis. newsmaker23://news?id=123
    push_image_used:   { type: DataTypes.TEXT, allowNull: true },              // final image yg dipakai di push
    push_hash:         { type: DataTypes.STRING(64), allowNull: true, unique: false }, // jejak dedupe opsional
  }, {
    tableName: 'news',
    timestamps: true,
    charset: 'utf8mb4',
    collate: 'utf8mb4_unicode_ci',

    indexes: [
      { unique: true, fields: ['link', 'language'], name: 'uniq_link_lang' },
      { fields: ['createdAt'] },
      { fields: ['language'] },
      { fields: ['category'] },
      { fields: ['published_at'] },
      { fields: ['push_state'] },
      { fields: ['push_sent_at'] },
      { fields: ['push_hash'] },
    ],

    defaultScope: { order: [['published_at','DESC'], ['createdAt','DESC']] },

    scopes: {
      needPush:   { where: { push_state: 'pending' } },
      pushed:     { where: { push_state: 'sent' } },
      byLang(lang){ return { where: { language: String(lang||'').toLowerCase() } }; },
    },

    hooks: {
      beforeValidate(instance) {
        // === normalisasi ringan
        const trim = (s) => (typeof s === 'string' ? s.trim() : s);
        instance.title       = trim(instance.title);
        instance.link        = trim(instance.link);
        instance.image       = trim(instance.image);
        instance.category    = trim(instance.category);
        instance.language    = trim(instance.language || 'en').toLowerCase();
        instance.source_name = trim(instance.source_name || 'Newsmaker 23');
        instance.source_url  = trim(instance.source_url || instance.link || '');
        instance.author      = instance.author ? String(instance.author).toLowerCase() : instance.author;

        // hapus trailing slash pada link/url
        const stripSlash = (u) => (typeof u === 'string' && u.length > 1 && u.endsWith('/') ? u.slice(0, -1) : u);
        instance.link = stripSlash(instance.link);
        instance.source_url = stripSlash(instance.source_url);

        // === auto-derive push fields kalau kosong ===
        // topic dari kategori
        if (!instance.push_topic) {
          const c = (instance.category || '').toLowerCase();
          let topic = 'news_all';
          if (c.includes('commodity')) topic = 'news_commodity';
          else if (c.includes('currenc')) topic = 'news_currencies';
          else if (c.includes('index'))   topic = 'news_index';
          else if (c.includes('crypto'))  topic = 'news_crypto';
          else if (c.includes('economy') || c.includes('fiscal')) topic = 'news_economy';
          else if (c.includes('analysis')) topic = 'news_analysis';
          instance.push_topic = topic;
        }

        // collapse key & deeplink — isi setelah ada id (di afterCreate), tapi buat “sementara”
        if (!instance.push_collapse_key) {
          const seed = `${instance.link}:${+new Date(instance.published_at||Date.now())}:${instance.language}`;
          instance.push_collapse_key = `news_${crypto.createHash('md5').update(seed).digest('hex').slice(0,10)}`;
        }
        if (!instance.push_deeplink) {
          // akan diperbarui di afterCreate agar pakai id asli
          instance.push_deeplink = 'newsmaker23://news';
        }

        // push_hash buat dedupe (server bisa cek/override)
        if (!instance.push_hash) {
          const basis = `${instance.title}|${instance.link}|${instance.language}|${instance.image||''}|${+new Date(instance.published_at)}`;
          instance.push_hash = crypto.createHash('sha256').update(basis).digest('hex');
        }

        // published_at safety
        const t = instance.published_at ? new Date(instance.published_at) : null;
        if (!t || Number.isNaN(+t)) instance.published_at = new Date();
      },

      afterCreate(instance) {
        // setelah ada ID, finalize deeplink & collapseKey yang lebih stabil
        if (instance.id) {
          if (!instance.push_deeplink || !/id=\d+/.test(instance.push_deeplink)) {
            instance.push_deeplink = `newsmaker23://news?id=${instance.id}`;
          }
          if (!instance.push_collapse_key || /^news_[0-9a-f]{10}$/.test(instance.push_collapse_key)) {
            instance.push_collapse_key = `news_${instance.id}`;
          }
          // simpan perubahan kecil ini tanpa memicu hooks lain
          return instance.update(
            { push_deeplink: instance.push_deeplink, push_collapse_key: instance.push_collapse_key },
            { hooks: false, silent: true }
          );
        }
      },
    },
  });

  // =================== INSTANCE METHODS (helper push) ===================

  /** Build payload FCM (topic-based). Dipakai server.js */
  News.prototype.buildFcmMessage = function buildFcmMessage() {
    const title  = String(this.title || '').slice(0, 110);
    const body   = String(this.summary || 'Tap to read more').slice(0, 230);
    const image  = this.image || undefined;
    const topic  = this.push_topic || 'news_all';
    const deeplink = this.push_deeplink || (this.id ? `newsmaker23://news?id=${this.id}` : 'newsmaker23://news');

    return {
      topic,
      notification: { title, body, image },
      data: {
        news_id: String(this.id || ''),
        url: deeplink,
        category: String(this.category || ''),
        click_action: 'FLUTTER_NOTIFICATION_CLICK',
      },
      android: {
        notification: { channelId: 'high_importance_channel', imageUrl: image, priority: 'HIGH', defaultSound: true },
        collapseKey: this.push_collapse_key || (this.id ? `news_${this.id}` : undefined),
      },
      apns: { payload: { aps: { sound: 'default' } }, fcmOptions: { imageUrl: image } },
    };
  };

  /** Tandai sukses kirim push */
  News.prototype.markPushed = function markPushed({ imageUsed } = {}) {
    return this.update({
      push_state: 'sent',
      push_sent_at: new Date(),
      push_error: null,
      push_image_used: imageUsed || this.image || null,
    }, { hooks: false });
  };

  /** Tandai gagal kirim push (simpan error message) */
  News.prototype.markPushFailed = function markPushFailed(errorMessage) {
    return this.update({
      push_state: 'failed',
      push_error: String(errorMessage || '').slice(0, 2000),
    }, { hooks: false });
  };

  /** Skip push untuk item ini */
  News.prototype.skipPush = function skipPush(reason = '') {
    return this.update({
      push_state: 'skipped',
      push_error: reason ? `skipped: ${String(reason).slice(0,200)}` : null,
    }, { hooks: false });
  };

  return News;
};
