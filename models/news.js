module.exports = (sequelize, DataTypes) => {
  const News = sequelize.define('News', {
    title: { type: DataTypes.TEXT, allowNull: false },
    link: { type: DataTypes.STRING(512), allowNull: false, unique: true },
    image: { type: DataTypes.TEXT },
    category: { type: DataTypes.STRING },
    date: { type: DataTypes.STRING },
    summary: { type: DataTypes.TEXT },
    detail: { type: DataTypes.TEXT },
    language: { type: DataTypes.STRING, allowNull: false },
  }, {
    tableName: 'news',
    timestamps: true,
  });
  return News;
};
