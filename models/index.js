const { Sequelize, DataTypes } = require('sequelize');
const sequelize = new Sequelize(process.env.DB_NAME, process.env.DB_USER, process.env.DB_PASS, {
  host: process.env.DB_HOST,
  dialect: 'mysql', // atau 'postgres', tergantung
  logging: false,
});

const NewsModel = require('./news');
const HistoricalDataModel = require('./historical');

const News = NewsModel(sequelize, DataTypes);
const HistoricalData = HistoricalDataModel(sequelize, DataTypes);

module.exports = {
  sequelize,
  News,
  HistoricalData
};
