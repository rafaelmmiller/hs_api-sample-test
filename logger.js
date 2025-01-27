const levels = {
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3
};

let currentLevel = levels[process.env.LOG_LEVEL] !== undefined ? levels[process.env.LOG_LEVEL] : levels.INFO;

const setLogLevel = (level) => {
  if (levels[level] !== undefined) {
    currentLevel = levels[level];
  } else {
    console.warn(`Invalid log level: ${level}`);
  }
};

const log = (level, message, ...args) => {
  if (levels[level] <= currentLevel) {
    const utcTime = new Date().toISOString();
    console.log(`[${utcTime}] [${level}] ${message}`, ...args);
  }
};

const error = (message, ...args) => log('ERROR', message, ...args);
const warn = (message, ...args) => log('WARN', message, ...args);
const info = (message, ...args) => log('INFO', message, ...args);
const debug = (message, ...args) => log('DEBUG', message, ...args);

module.exports = {
  setLogLevel,
  error,
  warn,
  info,
  debug
};