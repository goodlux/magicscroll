"""MagicScroll - Multi-modal data management and retrieval system."""

__version__ = "0.1.0"

from .config import settings
from .stores import storage
from .ms_entry import MSEntry, MSConversation, MSDocument, MSImage, MSCode, EntryType
from .magic_scroll import MagicScroll
from .ms_search import MSSearch
from .ms_types import SearchResult
from .fipa_acl import FIPAACLMessage, FIPAACLDatabase

# New migrated modules
from .ms_entity import EntityExtractor, EntityManager, ExtractedEntity
from .ms_milvus_store import MSMilvusStore
from .ms_sqlite_store import MSSQLiteStore

# Ingestor modules
from .ingestor import BaseIngestor, AnthropicIngestor

# CLI module
from .cli import MagicScrollCLI

# Digital Trinity integration
from .digital_trinity import (
    FIPAACLMessage as DTFIPAACLMessage,
    FIPAACLDatabase as DTFIPAACLDatabase,
    MessageAdapter,
    AgentProfile,
    ModelHandler,
    OpenAIModelHandler,
    AnthropicModelHandler,
    MultiModelChatManager
)

__all__ = [
    "settings", "storage", 
    "MSEntry", "MSConversation", "MSDocument", "MSImage", "MSCode", "EntryType",
    "MagicScroll", "MSSearch", "SearchResult",
    "FIPAACLMessage", "FIPAACLDatabase",
    # New migrated modules
    "EntityExtractor", "EntityManager", "ExtractedEntity",
    "MSMilvusStore", "MSSQLiteStore",
    # Ingestor modules
    "BaseIngestor", "AnthropicIngestor",
    # CLI
    "MagicScrollCLI",
    # Digital Trinity
    "DTFIPAACLMessage", "DTFIPAACLDatabase", "MessageAdapter",
    "AgentProfile", "ModelHandler", "OpenAIModelHandler", "AnthropicModelHandler",
    "MultiModelChatManager"
]
