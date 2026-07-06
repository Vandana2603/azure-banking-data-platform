"""
exceptions.py
=============
Custom exception hierarchy so calling jobs / ADF pipeline activities can
distinguish between failure types (and ADF can branch / alert accordingly).
"""


class PipelineError(Exception):
    """Base class for all pipeline-related exceptions."""


class SchemaValidationError(PipelineError):
    """Raised when incoming data does not match the expected schema."""


class DataQualityError(PipelineError):
    """Raised when data fails quality checks (nulls, duplicates, thresholds)."""


class ReferentialIntegrityError(PipelineError):
    """Raised when foreign-key style relationships are violated
    (e.g. a payment references an account_id that doesn't exist)."""


class IncrementalLoadError(PipelineError):
    """Raised when the incremental/watermark load logic fails."""


class ConfigurationError(PipelineError):
    """Raised when required configuration values are missing or invalid."""
