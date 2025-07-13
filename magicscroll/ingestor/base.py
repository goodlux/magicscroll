"""Base ingestor class for MagicScroll - defines the interface for all data source ingestors."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from ..ms_message import MSMessage
from ..ms_sqlite_store import MSSQLiteStore
from ..ms_entry import MSEntry, EntryType

logger = logging.getLogger(__name__)

class BaseIngestor(ABC):
    """
    Abstract base class for all MagicScroll ingestors.
    
    Defines the common interface for ingesting data from various sources
    (Anthropic Claude exports, Google Takeout, OpenAI exports, etc.)
    into the MagicScroll format.
    """
    
    def __init__(self, magic_scroll=None, db_path: Optional[str] = None):
        """
        Initialize the base ingestor.
        
        Args:
            magic_scroll: Optional MagicScroll instance for full integration
            db_path: Optional database path override
        """
        self.magic_scroll = magic_scroll
        
        # Initialize SQLite store with proper fallback and debugging
        logger.info(f"BaseIngestor init: magic_scroll={magic_scroll is not None}")
        if magic_scroll:
            logger.info(f"MagicScroll.sqlite_store={magic_scroll.sqlite_store is not None}")
        
        if magic_scroll and magic_scroll.sqlite_store:
            self.sqlite_store = magic_scroll.sqlite_store
            logger.info("Using SQLite store from MagicScroll instance")
        else:
            # Fallback: create our own SQLite store
            logger.info(f"Creating new SQLite store for ingestor with db_path={db_path}")
            try:
                import asyncio
                self.sqlite_store = asyncio.run(MSSQLiteStore.create(db_path))
                logger.info("Successfully created new SQLite store for ingestor")
            except Exception as e:
                logger.error(f"Failed to create SQLite store: {e}")
                self.sqlite_store = None
        
        # Verify SQLite store is working
        if self.sqlite_store is None:
            logger.error("CRITICAL: self.sqlite_store is None after initialization!")
        else:
            logger.info("SQLite store successfully initialized in BaseIngestor")
        
        # Tracking counters
        self.processed_conversations = 0
        self.processed_messages = 0
        self.errors = []
        
        # Subclass should set these
        self.source_name = "unknown"
        self.supported_formats = []
    
    @abstractmethod
    def parse_source_data(self, source_path: str) -> List[Dict[str, Any]]:
        """
        Parse the source data file into a standardized conversation format.
        
        Args:
            source_path: Path to the source data file
            
        Returns:
            List of conversation dictionaries in a standardized format
            
        Each conversation dict should contain:
        - id: Unique conversation identifier
        - title: Conversation title/name
        - created_at: ISO timestamp
        - updated_at: ISO timestamp  
        - messages: List of message dicts
        
        Each message dict should contain:
        - id: Unique message identifier
        - sender: Who sent the message (standardized to 'human'/'assistant'/model_name)
        - content: Text content of the message
        - created_at: ISO timestamp
        - metadata: Dict of additional metadata
        """
        pass
    
    @abstractmethod
    def extract_message_content(self, message: Dict[str, Any]) -> str:
        """
        Extract clean text content from a message.
        
        Args:
            message: Message dictionary from parse_source_data
            
        Returns:
            Clean text content string
        """
        pass
    
    def standardize_sender(self, raw_sender: str) -> str:
        """
        Standardize sender names to common format.
        
        Args:
            raw_sender: Raw sender identifier from source
            
        Returns:
            Standardized sender ('human', 'assistant', or specific model name)
        """
        # Default implementation - subclasses can override
        sender_lower = raw_sender.lower()
        
        if sender_lower in ['human', 'user', 'person']:
            return 'human'
        elif sender_lower in ['assistant', 'ai', 'bot']:
            return 'assistant'
        else:
            return raw_sender  # Keep original for specific models
    
    def convert_to_ms_message(
        self, 
        message: Dict[str, Any], 
        conversation_id: str,
        previous_message_id: Optional[str] = None
    ) -> MSMessage:
        """
        Convert a standardized message to MSMessage format.
        
        Args:
            message: Standardized message dictionary
            conversation_id: Conversation UUID
            previous_message_id: Previous message ID for threading
            
        Returns:
            MSMessage instance
        """
        sender = self.standardize_sender(message.get('sender', 'unknown'))
        content = message.get('content', '')  # Use already extracted content
        
        # Map to FIPA performatives
        if sender == 'human':
            performative = 'REQUEST'
            sender_id = 'user'
            receiver_id = 'assistant'
        elif sender == 'assistant':
            performative = 'INFORM'
            sender_id = 'assistant'
            receiver_id = 'user'
        else:
            performative = 'INFORM'
            sender_id = sender
            receiver_id = 'user'
        
        # Create MS message
        ms_msg = MSMessage(
            performative=performative,
            sender=sender_id,
            receiver=receiver_id,
            content=content,
            conversation_id=conversation_id,
            in_reply_to=previous_message_id,
            message_id=message.get('id')
        )
        
        # Add source-specific metadata
        ms_msg.metadata = {
            'source': self.source_name,
            'original_sender': message.get('sender', ''),
            'created_at': message.get('created_at', ''),
            **(message.get('metadata', {}))
        }
        
        return ms_msg
    
    def process_conversation(self, conversation: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single conversation into MSMessage format.
        
        Args:
            conversation: Standardized conversation dictionary
            
        Returns:
            Dictionary with conversation_id, title, message_count, messages
            Or None if processing failed
        """
        try:
            # Safety check for SQLite store
            if self.sqlite_store is None:
                error_msg = "CRITICAL: Cannot process conversation - SQLite store is None"
                logger.error(error_msg)
                self.errors.append(error_msg)
                return None
            
            conv_id = conversation.get('id')
            title = conversation.get('title', 'Untitled')
            
            # Always ensure we have a conversation ID
            if not conv_id:
                # Use MSSQLiteStore's conversation creation method
                if hasattr(self.sqlite_store, 'create_conversation'):
                    conv_id = self.sqlite_store.create_conversation(title=title)
                    logger.info(f"Created new conversation: {conv_id}")
                else:
                    import uuid
                    conv_id = str(uuid.uuid4())
                    logger.info(f"Generated conversation ID: {conv_id}")
            
            messages = conversation.get('messages', [])
            
            # Sort by timestamp to ensure proper order
            sorted_messages = sorted(
                messages,
                key=lambda m: m.get('created_at', '1970-01-01T00:00:00Z')
            )
            
            ms_messages = []
            previous_message_id = None
            
            for msg in sorted_messages:
                try:
                    ms_msg = self.convert_to_ms_message(
                        msg, conv_id, previous_message_id
                    )
                    
                    # Safety check before saving
                    if self.sqlite_store is None:
                        error_msg = f"SQLite store became None while processing message {msg.get('id', 'unknown')}"
                        logger.error(error_msg)
                        self.errors.append(error_msg)
                        continue
                    
                    # Save to SQLite store 
                    if hasattr(self.sqlite_store, 'save_message'):
                        self.sqlite_store.save_message(ms_msg)
                    else:
                        logger.warning("SQLite store doesn't have save_message method")
                        
                    ms_messages.append(ms_msg)
                    
                    previous_message_id = ms_msg.id
                    self.processed_messages += 1
                    
                except Exception as e:
                    error_msg = f"Error processing message {msg.get('id', 'unknown')}: {e}"
                    self.errors.append(error_msg)
                    logger.warning(error_msg)
            
            self.processed_conversations += 1
            
            return {
                'conversation_id': conv_id,
                'title': title,
                'message_count': len(ms_messages),
                'messages': ms_messages
            }
            
        except Exception as e:
            error_msg = f"Error processing conversation {conversation.get('id', 'unknown')}: {e}"
            self.errors.append(error_msg)
            logger.error(error_msg)
            return None
    
    async def create_ms_entry(self, conversation_data: Dict[str, Any]) -> Optional[MSEntry]:
        """
        Create an MSEntry for long-term storage and search with entity extraction.
        
        Args:
            conversation_data: Processed conversation data
            
        Returns:
            MSEntry instance or None if creation failed
        """
        if not self.magic_scroll:
            logger.warning("No MagicScroll instance - cannot create MSEntry")
            return None
        
        try:
            messages = conversation_data['messages']
            
            # Format as conversation text
            formatted_lines = []
            for msg in messages:
                sender = msg.metadata.get('original_sender', msg.sender)
                formatted_lines.append(f"{sender}: {msg.content}")
            
            conversation_text = '\n\n'.join(formatted_lines)
            
            # Extract entities using GLiNER
            try:
                from ..ms_entity import get_entity_extractor
                extractor = get_entity_extractor()
                entities_data = extractor.extract_for_conversation(conversation_text)
                logger.debug(f"Extracted {entities_data['entity_count']} entities for conversation {conversation_data['conversation_id']}")
            except Exception as e:
                logger.warning(f"Entity extraction failed: {e}")
                entities_data = None
            
            # Create MSConversation entry with entities
            from ..ms_entry import MSConversation
            ms_entry = MSConversation(
                content=conversation_text,
                metadata={
                    'live_conversation_id': conversation_data['conversation_id'],
                    'title': conversation_data['title'],
                    'message_count': conversation_data['message_count'],
                    'source': self.source_name,
                    'entities': entities_data['entities_by_type'] if entities_data else {},
                    'entity_count': entities_data['entity_count'] if entities_data else 0,
                    'entity_summary': extractor.get_entity_summary(entities_data) if entities_data else 'No entities extracted'
                }
            )
            
            # Save to MagicScroll
            entry_id = await self.magic_scroll.save_ms_entry(ms_entry)
            
            # Store entities in Kuzu graph database
            try:
                from ..ms_kuzu_store import store_entities_in_graph
                
                # Convert entity data to GLiNER format for Kuzu storage
                gliner_entities = []
                if entities_data and 'entities' in entities_data:
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
                    conversation_data['conversation_id'],
                    entry_id,
                    conversation_data['title']
                )
                
                logger.info(f"Stored entities in graph: {entity_counts}")
                
            except Exception as e:
                logger.warning(f"Failed to store entities in graph: {e}")
            
            logger.info(f"Created MSEntry {entry_id} for conversation {conversation_data['conversation_id']} with {entities_data['entity_count'] if entities_data else 0} entities")
            
            return ms_entry
            
        except Exception as e:
            error_msg = f"Error creating MSEntry: {e}"
            self.errors.append(error_msg)
            logger.error(error_msg)
            return None
    
    async def ingest(
        self,
        source_path: str,
        create_ms_entries: bool = False,
        limit_conversations: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Main ingestion method - processes source data into MagicScroll format.
        
        Args:
            source_path: Path to source data file
            create_ms_entries: Whether to create MSEntry objects for search
            limit_conversations: Optional limit on number of conversations to process
            
        Returns:
            Ingestion summary dictionary
        """
        logger.info(f"Starting {self.source_name} ingestion from {source_path}")
        
        # Reset counters
        self.processed_conversations = 0
        self.processed_messages = 0
        self.errors = []
        
        try:
            # Parse source data
            conversations = self.parse_source_data(source_path)
            logger.info(f"Found {len(conversations)} conversations")
            
            # Limit if requested
            if limit_conversations:
                conversations = conversations[:limit_conversations]
                logger.info(f"Limited to {len(conversations)} conversations")
            
            processed_conversations = []
            ms_entries = []
            
            # Process each conversation
            for conversation in conversations:
                try:
                    # Convert to MS messages
                    result = self.process_conversation(conversation)
                    if result:
                        processed_conversations.append(result)
                        
                        # Create MSEntry if requested
                        if create_ms_entries:
                            ms_entry = await self.create_ms_entry(result)
                            if ms_entry:
                                ms_entries.append(ms_entry)
                    
                    # Progress logging
                    if self.processed_conversations % 100 == 0:
                        logger.info(f"Processed {self.processed_conversations} conversations...")
                        
                except Exception as e:
                    self.errors.append(f"Failed to process conversation: {e}")
                    logger.warning(f"Skipping conversation due to error: {e}")
                    continue
            
            # Create summary
            summary = {
                'source': self.source_name,
                'source_path': source_path,
                'processed_conversations': self.processed_conversations,
                'processed_messages': self.processed_messages,
                'ms_entries_created': len(ms_entries) if create_ms_entries else 0,
                'errors': len(self.errors),
                'error_messages': self.errors[:10],  # First 10 errors
                'success': True
            }
            
            logger.info(f"{self.source_name} ingestion complete: {summary}")
            return summary
            
        except Exception as e:
            error_msg = f"Fatal error during {self.source_name} ingestion: {e}"
            logger.error(error_msg)
            
            return {
                'source': self.source_name,
                'source_path': source_path,
                'processed_conversations': self.processed_conversations,
                'processed_messages': self.processed_messages,
                'ms_entries_created': 0,
                'errors': len(self.errors) + 1,
                'error_messages': self.errors + [error_msg],
                'success': False
            }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get current ingestion summary."""
        return {
            'source': self.source_name,
            'processed_conversations': self.processed_conversations,
            'processed_messages': self.processed_messages,
            'errors': len(self.errors),
            'error_messages': self.errors[:5]
        }
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'sqlite_store') and self.sqlite_store:
            if hasattr(self.sqlite_store, 'close'):
                # Don't use asyncio.run() as we're already in an async context
                try:
                    import asyncio
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # We're in an async context, don't call asyncio.run()
                        logger.info("Skipping SQLite close - already in async context")
                    else:
                        asyncio.run(self.sqlite_store.close())
                except Exception as e:
                    logger.warning(f"Error closing SQLite store: {e}")
