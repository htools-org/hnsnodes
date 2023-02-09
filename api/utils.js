/**
 * Errors
 */

class BadRequestError extends Error {
  constructor(message) {
    super(message)
    this.statusCode = 400;
  }
}

class NotFoundError extends Error {
  constructor(message) {
    super(message)
    this.statusCode = 400;
  }
}

module.exports = {
  BadRequestError,
  NotFoundError,
}
