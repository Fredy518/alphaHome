"""tinydata exception hierarchy."""


class TinyDataError(Exception):
    """Base class for tinydata errors."""


class TinyDataDependencyError(TinyDataError):
    """Required optional dependency is missing."""


class TinyDataConfigError(TinyDataError):
    """Configuration is invalid or incomplete."""


class TinyDataAuthError(TinyDataError):
    """Tinysoft authentication failed."""


class TinyDataTimeoutError(TinyDataError):
    """Tinysoft call exceeded the configured timeout."""


class TinyDataQueryError(TinyDataError):
    """Tinysoft query failed."""


class TinyDataCodePoolError(TinyDataError):
    """A dataset needs codes but no valid code pool was available."""
