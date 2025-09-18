module.exports = (sequelize, DataTypes) => {
  const HistoricalData = sequelize.define('HistoricalData', {
    symbol: { type: DataTypes.STRING, allowNull: false },
    date: { type: DataTypes.STRING, allowNull: false },
    event: { type: DataTypes.STRING, allowNull: true },
    open: { type: DataTypes.FLOAT, allowNull: true },
    high: { type: DataTypes.FLOAT, allowNull: true },
    low: { type: DataTypes.FLOAT, allowNull: true },
    close: { type: DataTypes.FLOAT, allowNull: true },
    change: { type: DataTypes.STRING, allowNull: true },
    volume: { type: DataTypes.FLOAT, allowNull: true },
    openInterest: { type: DataTypes.FLOAT, allowNull: true }
  }, {
    tableName: 'historical_data',
    indexes: [
      {
        unique: true,
        fields: ['symbol', 'date']
      }
    ]
  });
  return HistoricalData;
};
