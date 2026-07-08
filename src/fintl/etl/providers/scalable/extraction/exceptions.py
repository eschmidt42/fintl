"""Exceptions raised by the Scalable Capital extraction utilities."""


class OllamaUnavailableError(Exception):
    """Raised when the ollama server cannot be reached."""


class OllamaModelUnavailableError(Exception):
    """Raised when the requested model is not present in the ollama instance."""


class OllamaInferenceError(Exception):
    """Raised when the ollama model runner fails during inference."""
