"""Constants for extraction activities."""

from enum import StrEnum, auto


class ModelProvider(StrEnum):
    """Model provider enum eheh."""

    ollama = auto()
    llama_swap = auto()


TIMEOUT = 2 * 60
LLAMA_SWAP_BASE_URL = "http://0.0.0.0:8080"
OLLAMA_BASE_URL = "http://localhost:11434"
