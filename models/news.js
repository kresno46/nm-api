// models/news.js
module.exports = (sequelize, DataTypes) => {
  const News = sequelize.define('News', {
    title:       { type: DataTypes.TEXT, allowNull: false },
    link:        { type: DataTypes.STRING(512), allowNull: false },
    image:       { type: DataTypes.TEXT, allowNull: true },
    category:    { type: DataTypes.STRING, allowNull: true },
    date:        { type: DataTypes.STRING, allowNull: true },     // raw date text from site (legacy)
    summary:     { type: DataTypes.TEXT, allowNull: true },
    detail:      { type: DataTypes.TEXT, allowNull: true },
    language:    { type: DataTypes.STRING(5), allowNull: false }, // 'en' / 'id'

    // === NEW (Play Console) ===
    source_name: { type: DataTypes.STRING, allowNull: false },     // e.g. "NewsMaker"
    source_url:  { type: DataTypes.TEXT,   allowNull: false },     // canonical/source link
    author:      { type: DataTypes.STRING, allowNull: true },      // e.g. "azf"
    author_name: { type: DataTypes.STRING, allowNull: true },      // e.g. "Azifah" ← ditambahkan
    published_at:{ type: DataTypes.DATE,   allowNull: false },     // normalized publish datetime
  }, {
    tableName: 'news',
    timestamps: true,
    indexes: [
      { unique: true, fields: ['link', 'language'], name: 'uniq_link_lang' },
      { fields: ['createdAt'] },
      { fields: ['language'] },
      { fields: ['category'] },
      { fields: ['published_at'] },
    ],
    hooks: {
      beforeValidate(instance) {
        if (instance.link) {
          instance.link = instance.link.trim();
          if (instance.link.length > 1 && instance.link.endsWith('/')) {
            instance.link = instance.link.slice(0, -1);
          }
        }
        if (instance.language) instance.language = instance.language.trim().toLowerCase();

        // fallback safety: pastikan kolom Play Console gak kosong
        if (!instance.source_name) instance.source_name = 'Newsmaker 23';
        if (!instance.source_url)  instance.source_url  = instance.link || '';
        if (!instance.published_at || isNaN(new Date(instance.published_at))) {
          instance.published_at = new Date();
        }
      },
    },
  });

  return News;
};
