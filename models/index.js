const { Sequelize, DataTypes } = require('sequelize');

const sequelize = new Sequelize(
  process.env.MYSQLDATABASE,
  process.env.MYSQLUSER,
  process.env.MYSQLPASSWORD,
  {
    host: process.env.MYSQLHOST,
    port: process.env.MYSQLPORT || 3306,
    dialect: 'mysql',
    logging: false
  }
);


const NewsModel = require('./news');
const HistoricalDataModel = require('./historical');

const News = NewsModel(sequelize, DataTypes);
const HistoricalData = HistoricalDataModel(sequelize, DataTypes);

module.exports = {
  sequelize,
  News,
  HistoricalData
};
