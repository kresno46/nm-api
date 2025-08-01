module.exports = (sequelize, DataTypes) => {
  const News = sequelize.define('News', {
    title: { type: DataTypes.STRING(255), allowNull: false },
    link: { type: DataTypes.STRING(512), allowNull: false, unique: true },
    image: { type: DataTypes.STRING(500) },
    category: { type: DataTypes.STRING },
    date: { type: DataTypes.STRING },
    summary: { type: DataTypes.STRING },
    detail: { type: DataTypes.STRING },
    language: { type: DataTypes.STRING, allowNull: false },
  }, {
    tableName: 'news',
    timestamps: true,
  });
  return News;
};
