"""Custom exceptions for the data cleaning pipeline."""


class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass


class DataIngestionError(PipelineError):
    """Raised when data ingestion fails."""
    pass


class DataCleaningError(PipelineError):
    """Raised when data cleaning fails."""
    pass


class DataValidationError(PipelineError):
    """Raised when data validation fails."""
    pass


class DataExportError(PipelineError):
    """Raised when data export fails."""
    pass
