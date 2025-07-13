"""Core MagicScroll system providing simple storage and search capabilities."""
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging

from .ms_entry import MSEntry, EntryType, MSConversation
from .ms_milvus_store import MSMilvusStore
from .ms_sqlite_store import MSSQLiteStore
from .ms_types import SearchResult
from .ms_message import MSMessage
from .config import settings

# Set up logging
logger = logging.getLogger(__name__)

class MagicScroll:
    """Core system for storing and searching chat conversations with context enrichment."""
    
    def __init__(self):
        """Initialize with config."""
        self.ms_store = None
        self.search_engine = None
        self.sqlite_store = None

    @classmethod 
    async def create(cls, storage_type: str = "milvus") -> 'MagicScroll':
        """Create a new MagicScroll using specified storage type."""
        magicscroll = cls()
        await magicscroll.initialize(storage_type)
        return magicscroll
    
    async def initialize(self, storage_type: str = "milvus") -> None:
        """Initialize the components with clean architecture."""
        logger.info(f"Initializing MagicScroll with {storage_type} storage...")
        
        # STEP 1: Initialize SQLite store for live conversations AND MSEntries
        logger.info("Initializing SQLite store for live conversations and entries...")
        try:
            self.sqlite_store = await MSSQLiteStore.create()
            logger.info("✅ SQLite store initialized successfully")
        except Exception as e:
            logger.error(f"CRITICAL: SQLite store initialization failed: {e}")
            import traceback
            logger.error(f"SQLite store traceback: {traceback.format_exc()}")
            raise RuntimeError(f"Cannot proceed without SQLite store: {e}")
        
        # STEP 2: Initialize the MS store for long-term vector search (if not SQLite-only)
        if storage_type.lower() != "sqlite":
            logger.info("Initializing MS store for long-term vector storage...")
            try:
                if storage_type.lower() == "milvus":
                    logger.info("Creating MSMilvusStore...")
                    self.ms_store = await MSMilvusStore.create()
                    logger.info("✅ Using Milvus storage")
                else:
                    logger.warning(f"Unknown storage type {storage_type}, defaulting to Milvus")
                    self.ms_store = await MSMilvusStore.create()
                    logger.info("✅ Using Milvus storage (default)")
                    
                # Verify the store was created
                if self.ms_store:
                    logger.info(f"MS store successfully initialized: {type(self.ms_store).__name__}")
                else:
                    logger.error("MS store is None after creation!")
                    
            except Exception as e:
                logger.error(f"MS store initialization failed: {e}")
                import traceback
                logger.error(f"MS store traceback: {traceback.format_exc()}")
                logger.warning("Continuing with SQLite-only mode")
                self.ms_store = None
        else:
            logger.info("Using SQLite-only mode - SQLite will handle both live and long-term storage")
            self.ms_store = self.sqlite_store  # Use SQLite for everything
        
        # STEP 3: Initialize the search engine (depends on MS store)
        try:
            if self.ms_store:
                from .ms_search import MSSearch
                self.search_engine = MSSearch(self)
                logger.info("Search engine initialized")
            else:
                logger.warning("No MS store available - skipping search engine")
                self.search_engine = None
        except Exception as e:
            logger.warning(f"Search engine initialization failed: {e}")
            self.search_engine = None
        
        # Verify critical components
        if self.sqlite_store is None:
            raise RuntimeError("CRITICAL: SQLite store is None after initialization")
        
        logger.info("🪄 MagicScroll ready to unroll!")
        logger.info(f"Components status: sqlite_store={self.sqlite_store is not None}, ms_store={self.ms_store is not None}, search_engine={self.search_engine is not None}")
        
    # ===============================================
    # LIVE CONVERSATION METHODS (using SQLite store)
    # ===============================================
    
    def create_live_conversation(self, title: Optional[str] = None, metadata: Optional[Dict] = None) -> str:
        """Create a new live conversation."""
        if not self.sqlite_store:
            logger.warning("SQLite store not initialized")
            return ""
        return self.sqlite_store.create_conversation(title, metadata)
    
    def save_live_message(self, message: MSMessage) -> None:
        """Save a message to live storage."""
        if not self.sqlite_store:
            logger.warning("SQLite store not initialized")
            return
        self.sqlite_store.save_message(message)
    
    def get_live_conversation_messages(self, conversation_id: str) -> List[MSMessage]:
        """Get messages from a live conversation."""
        if not self.sqlite_store:
            logger.warning("SQLite store not initialized")
            return []
        return self.sqlite_store.get_conversation_messages(conversation_id)
    
    def end_live_conversation(self, conversation_id: str) -> None:
        """Mark a live conversation as ended."""
        if not self.sqlite_store:
            logger.warning("SQLite store not initialized")
            return
        self.sqlite_store.end_conversation(conversation_id)
    
    def get_live_conversation_info(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """Get live conversation metadata."""
        if not self.sqlite_store:
            logger.warning("SQLite store not initialized")
            return None
        return self.sqlite_store.get_conversation_info(conversation_id)
    
    def get_recent_live_conversations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent live conversations."""
        if not self.sqlite_store:
            logger.warning("SQLite store not initialized")
            return []
        return self.sqlite_store.get_recent_conversations(limit)
    
    # =================================================
    # LONG-TERM STORAGE METHODS (using MS stores)
    # =================================================
    
    async def save_ms_entry(self, entry: MSEntry) -> str:
        """Save an entry to long-term storage."""
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
        """Get an entry from long-term storage."""
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
        """Search entries in long-term storage using vector search."""
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
        """Get recent entries from long-term storage."""
        if not self.ms_store or not hasattr(self.ms_store, 'get_recent_entries'):
            logger.warning("Recent entries retrieval not available")
            return []
            
        try:
            entries = await self.ms_store.get_recent_entries(hours, entry_types, limit)
            return entries
        except Exception as e:
            logger.error(f"Error retrieving recent entries: {e}")
            return []

    # ==================================================
    # CONVERSATION LIFECYCLE (Live → Long-term)
    # ==================================================
    
    async def archive_conversation(self, conversation_id: str, metadata: Optional[Dict] = None) -> str:
        """Move a completed live conversation to long-term storage with entity extraction."""
        if not self.sqlite_store:
            logger.warning("SQLite store not initialized")
            return ""
            
        try:
            # Get messages from live conversation
            messages = self.sqlite_store.get_conversation_messages(conversation_id)
            
            if not messages:
                logger.warning(f"No messages found for conversation {conversation_id}")
                return ""
            
            # Get conversation info
            conv_info = self.sqlite_store.get_conversation_info(conversation_id)
            
            # Format the conversation for storage
            formatted_content = self._format_messages(messages)
            
            # Extract entities using the same pipeline as ingestion
            entities_data = None
            try:
                from .ms_entity import get_entity_extractor
                extractor = get_entity_extractor()
                entities_data = extractor.extract_for_conversation(formatted_content)
                logger.debug(f"Extracted {entities_data['entity_count']} entities for conversation {conversation_id}")
            except Exception as e:
                logger.warning(f"Entity extraction failed: {e}")
                entities_data = None
            
            # Create conversation entry with entities
            entry = MSConversation(
                content=formatted_content,
                metadata={
                    "live_conversation_id": conversation_id,
                    "title": conv_info.get('title', 'Archived Conversation') if conv_info else 'Archived Conversation',
                    "message_count": len(messages),
                    "participants": list(set(msg.sender for msg in messages if msg.sender)),
                    "entities": entities_data['entities_by_type'] if entities_data else {},
                    "entity_count": entities_data['entity_count'] if entities_data else 0,
                    "entity_summary": extractor.get_entity_summary(entities_data) if entities_data else 'No entities extracted',
                    **(metadata or {})
                }
            )
            
            # Save to long-term storage (Milvus)
            entry_id = await self.save_ms_entry(entry)
            
            # Store entities in Kuzu graph database (same as ingestion)
            if entities_data:
                try:
                    from .ms_kuzu_store import store_entities_in_graph
                    
                    # Convert entity data to GLiNER format for Kuzu storage
                    gliner_entities = []
                    if 'entities' in entities_data:
                        for entity in entities_data['entities']:
                            gliner_entities.append({
                                'text': entity.text,
                                'label': entity.label,
                                'score': entity.confidence,
                                'start': entity.start,
                                'end': entity.end
                            })
                    
                    entity_counts = store_entities_in_graph(
                        gliner_entities,
                        conversation_id,
                        entry_id,
                        conv_info.get('title', 'Archived Conversation') if conv_info else 'Archived Conversation'
                    )
                    
                    logger.info(f"Stored entities in graph: {entity_counts}")
                    
                except Exception as e:
                    logger.warning(f"Failed to store entities in graph: {e}")
            
            return entry_id
            
        except Exception as e:
            logger.error(f"Error archiving conversation {conversation_id}: {e}")
            return ""
    
    def _format_messages(self, messages: List[MSMessage]) -> str:
        """Format messages into a storable conversation format."""
        formatted = []
        
        for msg in messages:
            sender = msg.sender
            content = msg.content
            formatted.append(f"{sender}: {content}")
            
        return "\n\n".join(formatted)

    async def close(self) -> None:
        """Close connections."""
        if self.ms_store and hasattr(self.ms_store, 'close') and self.ms_store != self.sqlite_store:
            await self.ms_store.close()
            logger.info("MagicScroll store connections closed")
        if self.sqlite_store:
            await self.sqlite_store.close()
            logger.info("SQLite store connection closed")
