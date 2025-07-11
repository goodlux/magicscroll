"""MagicScroll Ingestors - Modular data source ingestion for various chat/conversation formats."""

from .base import BaseIngestor
from .anthropic import AnthropicIngestor

__all__ = [
    'BaseIngestor',
    'AnthropicIngestor'
]
