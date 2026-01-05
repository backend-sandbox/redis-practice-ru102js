const winston = require('winston');
const config = require('better-config');

const customFormat = winston.format.printf(({ level, message, timestamp }) => {
  return `\n${timestamp} [${level}]: ${message}`;
});

const logger = winston.createLogger({
  level: config.get('application.logLevel'),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.colorize(),
        customFormat,
      ),
    }),
  ],
});

logger.stream = {
  write: (message) => {
    // Removes double newline issue with piping morgan server request
    // log through winston logger.
    logger.info(message.length > 0 ? message.substring(0, message.length - 1) : message);
  },
};

module.exports = logger;
