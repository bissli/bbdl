"""Custom exceptions for the bbdl package."""

__all__ = [
    'BbdlError',
    'BbdlConnectionError',
    'BbdlTimeoutError',
    'BbdlParseError',
    'BbdlValidationError',
]


class BbdlError(Exception):
    """Base exception for all bbdl errors."""


class BbdlConnectionError(BbdlError):
    """Raised when connection to Bloomberg SFTP fails."""


class BbdlTimeoutError(BbdlError):
    """Raised when waiting for Bloomberg response times out."""


class BbdlParseError(BbdlError):
    """Raised when parsing Bloomberg response fails."""


class BbdlValidationError(BbdlError):
    """Raised when input validation fails."""
