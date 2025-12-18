class DolphinDBIntegrationError(RuntimeError):
    """Base error for DolphinDB integration."""


class DolphinDBNotInstalledError(DolphinDBIntegrationError):
    """Raised when the `dolphindb` Python package is missing."""


class DolphinDBConnectionError(DolphinDBIntegrationError):
    """Raised when a DolphinDB connection cannot be established."""


class DolphinDBScriptError(DolphinDBIntegrationError):
    """Raised when running a DolphinDB script fails."""

