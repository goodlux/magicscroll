"""
SQLite storage for MagicScroll - handles live conversations only.
"""

import sqlite3
import json
import uuid
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta
from pathlib import Path
import logging

from .ms_message import MSMessage
from .config import settings
from .db.schemas.sqlite_schema import SQLiteSchema

logger = logging.getLogger(__name__)

class MSSQLiteStore:
    """SQLite storage for live conversations only."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize SQLite storage using the authoritative schema."""
        self.db_path = db_path or str(settings.sqlite_path)
        
        # Use the authoritative schema to get connection
        try:
            self.conn = SQLiteSchema.get_connection(Path(self.db_path))
            logger.info(f"SQLite store initialized using authoritative schema at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize with authoritative schema: {e}")
            raise
        
        # Set up embedding model - using sentence-transformers (for future use)
        try:
            from sentence_transformers import SentenceTransformer
            self.embed_model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("Sentence transformers model loaded")
        except ImportError:
            logger.warning("sentence-transformers not installed")
            self.embed_model = None
    
    @classmethod
    async def create(cls, db_path: Optional[str] = None) -> 'MSSQLiteStore':
        """Factory method to create store instance."""
        return cls(db_path)
    
    # ============================================
    # LIVE MESSAGE METHODS (using fipa_messages)
    # ============================================
    
    def save_message(self, message: MSMessage) -> None:
        """
        Save a live message to the database.
        
        Args:
            message: The message to save
        """
        cursor = self.conn.cursor()
        data = message.to_dict()
        
        # Ensure we have all the fields for the fipa_messages table
        # Add speaker field (copy from sender for compatibility)
        if 'speaker' not in data:
            data['speaker'] = data['sender']
        
        # Use created_at for both created_at and timestamp fields
        if 'timestamp' not in data:
            data['timestamp'] = data['created_at']
        
        # Convert metadata to JSON if it's not already
        if 'metadata' not in data or data['metadata'] is None:
            data['metadata'] = json.dumps({})
        elif isinstance(data['metadata'], dict):
            data['metadata'] = json.dumps(data['metadata'])
        
        # Insert into fipa_messages table
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = list(data.values())
        
        sql = f"INSERT OR REPLACE INTO fipa_messages ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, values)
        self.conn.commit()
        logger.info(f"Message {message.id} saved to fipa_messages")
    
    def get_message(self, message_id: str) -> Optional[MSMessage]:
        """
        Retrieve a message by its ID.
        
        Args:
            message_id: The ID of the message to retrieve
            
        Returns:
            The message if found, otherwise None
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM fipa_messages WHERE message_id = ?", (message_id,))
        
        row = cursor.fetchone()
        if row is None:
            return None
            
        column_names = [description[0] for description in cursor.description]
        data = dict(zip(column_names, row))
        
        return MSMessage.from_dict(data)
    
    def get_conversation_messages(self, conversation_id: str) -> List[MSMessage]:
        """
        Retrieve all messages in a conversation.
        
        Args:
            conversation_id: The ID of the conversation
            
        Returns:
            List of messages in the conversation, ordered by timestamp
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM fipa_messages WHERE conversation_id = ? ORDER BY created_at",
            (conversation_id,)
        )
        
        messages = []
        column_names = [description[0] for description in cursor.description]
        
        for row in cursor.fetchall():
            data = dict(zip(column_names, row))
            messages.append(MSMessage.from_dict(data))
            
        return messages
    
    def create_conversation(self, title: Optional[str] = None, metadata: Optional[Dict] = None) -> str:
        """
        Create a new conversation.
        
        Args:
            title: An optional title for the conversation
            metadata: Optional metadata for the conversation
            
        Returns:
            The ID of the newly created conversation
        """
        conversation_id = str(uuid.uuid4())
        cursor = self.conn.cursor()
        
        now = datetime.now().isoformat()
        title = title or f"Conversation {now}"
        metadata_json = json.dumps(metadata or {})
        
        # Insert into fipa_conversations table using WORKING schema
        cursor.execute(
            """INSERT INTO fipa_conversations 
               (conversation_id, title, start_time, end_time, created_at, updated_at, 
                account_uuid, message_count, total_tokens, metadata) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (conversation_id, title, now, now, now, now, '', 0, 0, metadata_json)
        )
        
        self.conn.commit()
        logger.info(f"Conversation {conversation_id} created")
        return conversation_id
    
    def end_conversation(self, conversation_id: str) -> None:
        """
        Mark a conversation as ended and update its metadata.
        
        Args:
            conversation_id: The ID of the conversation to end
        """
        cursor = self.conn.cursor()
        
        now = datetime.now().isoformat()
        
        # Update the conversation with final counts
        cursor.execute(
            """UPDATE fipa_conversations 
               SET end_time = ?, 
                   updated_at = ?,
                   message_count = (SELECT COUNT(*) FROM fipa_messages WHERE conversation_id = ?)
               WHERE conversation_id = ?""",
            (now, now, conversation_id, conversation_id)
        )
        
        self.conn.commit()
        logger.info(f"Conversation {conversation_id} ended")
    
    def get_conversation_info(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """
        Get conversation metadata.
        
        Args:
            conversation_id: The ID of the conversation
            
        Returns:
            Conversation metadata if found, otherwise None
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM fipa_conversations WHERE conversation_id = ?", (conversation_id,))
        
        row = cursor.fetchone()
        if row is None:
            return None
            
        column_names = [description[0] for description in cursor.description]
        return dict(zip(column_names, row))
    
    def get_recent_conversations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent conversations.
        
        Args:
            limit: Maximum number of conversations to return
            
        Returns:
            List of recent conversation metadata
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT * FROM fipa_conversations 
               ORDER BY updated_at DESC 
               LIMIT ?""",
            (limit,)
        )
        
        conversations = []
        column_names = [description[0] for description in cursor.description]
        
        for row in cursor.fetchall():
            conversations.append(dict(zip(column_names, row)))
            
        return conversations
    
    async def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed")

    def __del__(self):
        """Make sure connection is closed on deletion."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


# Convenience function to get SQLite store instance
def get_sqlite_store() -> MSSQLiteStore:
    """Get a SQLite store instance using the configured path."""
    import asyncio
    return asyncio.run(MSSQLiteStore.create())
