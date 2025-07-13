"""MagicScroll - Multi-modal data management and retrieval system."""

__version__ = "0.1.0"

from .config import settings
from .ms_entry import MSEntry, MSConversation, MSDocument, MSImage, MSCode, EntryType
from .magicscroll import MagicScroll
from .ms_search import MSSearch
from .ms_types import SearchResult

# Clean message and store classes
from .ms_message import MSMessage
from .ms_sqlite_store import MSSQLiteStore, get_sqlite_store
from .ms_milvus_store import MSMilvusStore

# Entity extraction
from .ms_entity import EntityExtractor, ExtractedEntity

# Ingestor modules
from .ingestor import BaseIngestor, AnthropicIngestor

# CLI module
from .cli import MagicScrollCLI

# Database management (keep the useful parts)
from .db.database_cli import DatabaseCLI


__all__ = [
    "settings",
    "MSEntry", "MSConversation", "MSDocument", "MSImage", "MSCode", "EntryType",
    "MagicScroll", "MSSearch", "SearchResult",
    # Clean classes
    "MSMessage", "MSSQLiteStore", "get_sqlite_store",
    "MSMilvusStore",
    # Entity extraction
    "EntityExtractor", "ExtractedEntity",
    # Ingestor modules
    "BaseIngestor", "AnthropicIngestor",
    # CLI
    "MagicScrollCLI",
    # Database management
    "DatabaseCLI"
]
