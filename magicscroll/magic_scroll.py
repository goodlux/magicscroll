"""Core MagicScroll system providing simple storage and search capabilities."""
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging

from .ms_entry import MSEntry, EntryType, MSConversation
from .ms_milvus_store import MSMilvusStore
from .ms_sqlite_store import MSSQLiteStore
from .ms_types import SearchResult
from .fipa_acl import FIPAACLDatabase, FIPAACLMessage
from .config import settings

# Set up logging
logger = logging.getLogger(__name__)

class MagicScroll:
    """Core system for storing and searching chat conversations with context enrichment."""
    
    def __init__(self):
        """Initialize with config."""
        self.ms_store = None
        self.search_engine = None
        self.fipa_db = None

    @classmethod 
    async def create(cls, storage_type: str = "milvus") -> 'MagicScroll':
        """Create a new MagicScroll using specified storage type."""
        magicscroll = cls()
        await magicscroll.initialize(storage_type)
        return magicscroll
    
    async def initialize(self, storage_type: str = "milvus") -> None:
        """Initialize the components with better error handling."""
        try:
            logger.info(f"Initializing MagicScroll with {storage_type} storage...")
            
            # Initialize the store based on configuration
            if storage_type.lower() == "sqlite":
                self.ms_store = await MSSQLiteStore.create()
                logger.info("Using SQLite storage with vector capabilities")
            else:
                # Default to Milvus
                self.ms_store = await MSMilvusStore.create()
                logger.info("Using Milvus storage")
            
            # Initialize the search engine
            from .ms_search import MSSearch
            self.search_engine = MSSearch(self)
            
            # Initialize FIPA database
            self.fipa_db = FIPAACLDatabase()
            
            logger.info("MagicScroll ready to unroll!")
        
        except Exception as e:
            logger.error(f"Failed to initialize MagicScroll: {str(e)}")
            # Create a minimal functional object instead of raising
            self.ms_store = None
            self.search_engine = None
            self.fipa_db = None
            logger.warning("MagicScroll running in minimal mode")
        
    async def save_ms_entry(self, entry: MSEntry) -> str:
        """Save an entry through the store."""
        if not self.ms_store:
            logger.warning("Cannot save entry - MagicScroll store not initialized")
            return entry.id  # Return ID but don't save

        try:
            if not await self.ms_store.save_ms_entry(entry):
                logger.error("Failed to write entry to store")
                return entry.id  # Return ID even if save failed
            
            logger.info(f"Successfully saved entry {entry.id} to store")
            return entry.id
        except Exception as e:
            logger.error(f"Error saving entry: {e}")
            return entry.id

    async def get_ms_entry(self, entry_id: str) -> Optional[MSEntry]:
        """Get an entry from the store."""
        if not self.ms_store:
            logger.warning("Cannot retrieve entry - MagicScroll store not initialized")
            return None
            
        try:
            entry = await self.ms_store.get_ms_entry(entry_id)
            if entry:
                logger.info(f"Successfully retrieved entry {entry_id}")
            else:
                logger.warning(f"Entry {entry_id} not found in store")
            return entry
        except Exception as e:
            logger.error(f"Error retrieving entry: {e}")
            return None

    async def search(
        self,
        query: str,
        entry_types: Optional[List[EntryType]] = None,
        temporal_filter: Optional[Dict[str, datetime]] = None,
        limit: int = 5
    ) -> List[SearchResult]:
        """Search entries in the scroll using vector search."""
        if not self.search_engine:
            logger.warning("Search engine not available")
            return []
            
        try:
            logger.info(f"Searching with query: '{query}', limit={limit}")
            if entry_types:
                logger.info(f"Filtering by entry types: {[t.value for t in entry_types]}")
            if temporal_filter:
                logger.info(f"Filtering by time window: {temporal_filter}")
                
            # Use MSSearch to perform the search
            results = await self.search_engine.search(
                query=query,
                entry_types=entry_types,
                temporal_filter=temporal_filter,
                limit=limit
            )
            
            logger.info(f"Search returned {len(results)} results")
            return results
        except Exception as e:
            logger.error(f"Error in search: {e}")
            return []

    async def search_conversation(
        self,
        message: str,
        temporal_filter: Optional[Dict[str, datetime]] = None,
        limit: int = 3
    ) -> List[SearchResult]:
        """Search for conversation context using semantic similarity."""
        if not self.search_engine:
            logger.warning("Search engine not available")
            return []
            
        try:
            logger.info(f"Searching for conversation context with: '{message[:50]}...'")
            
            # Use MSSearch's conversation-optimized search
            results = await self.search_engine.conversation_context_search(
                message=message,
                temporal_filter=temporal_filter,
                limit=limit
            )
            
            logger.info(f"Conversation search returned {len(results)} results")
            return results
        except Exception as e:
            logger.error(f"Error in conversation search: {e}")
            return []

    async def get_recent(
        self,
        hours: Optional[int] = None,
        entry_types: Optional[List[EntryType]] = None,
        limit: int = 10
    ) -> List[MSEntry]:
        """Get recent entries."""
        if not self.ms_store or not hasattr(self.ms_store, 'get_recent_entries'):
            logger.warning("Recent entries retrieval not available")
            return []
            
        try:
            entries = await self.ms_store.get_recent_entries(hours, entry_types, limit)
            return entries
        except Exception as e:
            logger.error(f"Error retrieving recent entries: {e}")
            return []

    # FIPA-related methods for scRAMble integration
    def create_fipa_conversation(self, title: Optional[str] = None, metadata: Optional[Dict] = None) -> str:
        """Create a new FIPA conversation."""
        if not self.fipa_db:
            logger.warning("FIPA database not initialized")
            return ""
        return self.fipa_db.create_conversation(title)
    
    def save_fipa_message(self, message: FIPAACLMessage) -> None:
        """Save a FIPA message."""
        if not self.fipa_db:
            logger.warning("FIPA database not initialized")
            return
        self.fipa_db.save_message(message)
    
    def get_fipa_conversation(self, conversation_id: str) -> List[FIPAACLMessage]:
        """Get messages from a FIPA conversation."""
        if not self.fipa_db:
            logger.warning("FIPA database not initialized")
            return []
        return self.fipa_db.get_conversation_messages(conversation_id)
        
    async def save_fipa_conversation_to_ms(self, conversation_id: str, metadata: Optional[Dict] = None) -> str:
        """Save FIPA conversation to MagicScroll long-term memory."""
        if not self.fipa_db:
            logger.warning("FIPA database not initialized")
            return ""
            
        messages = self.fipa_db.get_conversation_messages(conversation_id)
        
        # Format the conversation for storage
        formatted_content = self._format_fipa_conversation(messages)
        
        # Create conversation entry
        entry = MSConversation(
            content=formatted_content,
            metadata={
                "fipa_conversation_id": conversation_id,
                "message_count": len(messages),
                "participants": list(set(msg.sender for msg in messages if msg.sender)),
                **(metadata or {})
            }
        )
        
        # Add to the index
        return await self.save_ms_entry(entry)
    
    def _format_fipa_conversation(self, messages: List[FIPAACLMessage]) -> str:
        """Format FIPA messages into a storable conversation format."""
        formatted = []
        
        for msg in messages:
            sender = msg.sender
            content = msg.content
            formatted.append(f"{sender}: {content}")
            
        return "\n\n".join(formatted)

    async def close(self) -> None:
        """Close connections."""
        if self.ms_store and hasattr(self.ms_store, 'close'):
            await self.ms_store.close()
            logger.info("MagicScroll store connections closed")
        if self.fipa_db:
            self.fipa_db.close()
            logger.info("FIPA database connection closed")
