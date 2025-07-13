"""MagicScroll - Multi-modal data management and retrieval system."""

__version__ = "0.1.0"

from .config import settings
from .stores import storage
from .ms_entry import MSEntry, MSConversation, MSDocument, MSImage, MSCode, EntryType
from .magicscroll import MagicScroll
from .ms_search import MSSearch
from .ms_types import SearchResult
from .fipa_acl import FIPAACLMessage, FIPAACLDatabase

# New migrated modules
from .ms_entity import EntityExtractor, ExtractedEntity
from .ms_milvus_store import MSMilvusStore
from .ms_sqlite_store import MSSQLiteStore

# Ingestor modules
from .ingestor import BaseIngestor, AnthropicIngestor

# CLI module
from .cli import MagicScrollCLI


__all__ = [
    "settings", "storage", 
    "MSEntry", "MSConversation", "MSDocument", "MSImage", "MSCode", "EntryType",
    "MagicScroll", "MSSearch", "SearchResult",
    "FIPAACLMessage", "FIPAACLDatabase",
    # New migrated modules
    "EntityExtractor", "ExtractedEntity",
    "MSMilvusStore", "MSSQLiteStore",
    # Ingestor modules
    "BaseIngestor", "AnthropicIngestor",
    # CLI
    "MagicScrollCLI"
]
