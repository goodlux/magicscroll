"""Claude Chat Ingestor for MagicScroll using FIPA-ACL message protocol."""

import sqlite3
import json
import uuid
from datetime import datetime, UTC
from typing import List, Dict, Any, Optional
from pathlib import Path

from .config import settings


class FIPAMessage:
    """FIPA-ACL message representation for MagicScroll."""
    
    def __init__(self, 
                 sender: str,
                 receiver: str, 
                 content: str,
                 performative: str = "INFORM",
                 content_type: str = "text/plain",
                 reply_to: Optional[str] = None,
                 metadata: Optional[Dict[str, Any]] = None):
        self.message_id = str(uuid.uuid4())
        self.sender = sender
        self.receiver = receiver
        self.content = content
        self.performative = performative
        self.content_type = content_type
        self.timestamp = datetime.now(UTC).isoformat()
        self.reply_to = reply_to
        self.metadata = metadata or {}


class ClaudeChatIngestor:
    """Ingest Claude conversations into MagicScroll using FIPA-ACL protocol."""
    
    def __init__(self):
        self.db_path = settings.sqlite_path
        
    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection with proper configuration."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    
    def create_conversation(self, 
                          participants: List[str],
                          protocol: str = "claude-chat",
                          metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create a new FIPA conversation for Claude chat."""
        conversation_id = str(uuid.uuid4())
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """INSERT INTO fipa_conversations 
               (conversation_id, protocol, start_time, participants, metadata)
               VALUES (?, ?, ?, ?, ?)""",
            (
                conversation_id,
                protocol,
                datetime.now(UTC).isoformat(),
                json.dumps(participants),
                json.dumps(metadata or {})
            )
        )
        
        conn.commit()
        conn.close()
        return conversation_id
    
    def add_message(self, 
                   conversation_id: str,
                   message: FIPAMessage,
                   thread_depth: int = 0) -> str:
        """Add a FIPA message to the conversation."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """INSERT INTO fipa_messages 
               (message_id, conversation_id, sender, receiver, performative, 
                content, content_type, timestamp, reply_to, thread_depth, 
                embedding_status, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                message.message_id,
                conversation_id,
                message.sender,
                message.receiver,
                message.performative,
                message.content,
                message.content_type,
                message.timestamp,
                message.reply_to,
                thread_depth,
                "pending",  # Will be processed for embeddings later
                json.dumps(message.metadata)
            )
        )
        
        conn.commit()
        conn.close()
        return message.message_id
        
    def ingest_claude_exchange(self,
                             human_message: str,
                             claude_response: str,
                             conversation_id: Optional[str] = None,
                             human_metadata: Optional[Dict[str, Any]] = None,
                             claude_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """Ingest a human-Claude message exchange."""
        
        # Create conversation if not provided
        if conversation_id is None:
            conversation_id = self.create_conversation(
                participants=["human", "claude"],
                metadata={"source": "claude-desktop", "ingestion_time": datetime.now(UTC).isoformat()}
            )
        
        # Create human message
        human_msg = FIPAMessage(
            sender="human",
            receiver="claude",
            content=human_message,
            performative="REQUEST",  # Human typically requests information/action
            metadata=human_metadata or {}
        )
        
        # Create Claude response
        claude_msg = FIPAMessage(
            sender="claude", 
            receiver="human",
            content=claude_response,
            performative="INFORM",  # Claude typically informs/responds
            reply_to=human_msg.message_id,  # Link the response
            metadata=claude_metadata or {}
        )
        
        # Add messages to database
        human_msg_id = self.add_message(conversation_id, human_msg, thread_depth=0)
        claude_msg_id = self.add_message(conversation_id, claude_msg, thread_depth=1)
        
        return {
            "conversation_id": conversation_id,
            "human_message_id": human_msg_id,
            "claude_message_id": claude_msg_id
        }
    
    def get_conversation_messages(self, conversation_id: str) -> List[Dict[str, Any]]:
        """Retrieve all messages from a conversation in chronological order."""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(
            """SELECT * FROM fipa_messages 
               WHERE conversation_id = ? 
               ORDER BY timestamp, thread_depth""",
            (conversation_id,)
        )
        
        messages = []
        for row in cursor.fetchall():
            message = dict(row)
            message["metadata"] = json.loads(message["metadata"])
            messages.append(message)
        
        conn.close()
        return messages
    
    def get_conversation_context(self, conversation_id: str) -> Dict[str, Any]:
        """Get conversation metadata and participant info."""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT * FROM fipa_conversations WHERE conversation_id = ?",
            (conversation_id,)
        )
        
        row = cursor.fetchone()
        if row:
            conv = dict(row)
            conv["participants"] = json.loads(conv["participants"])
            conv["metadata"] = json.loads(conv["metadata"])
            return conv
        
        conn.close()
        return {}
    
    def close_conversation(self, conversation_id: str) -> bool:
        """Mark a conversation as ended."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "UPDATE fipa_conversations SET end_time = ? WHERE conversation_id = ?",
            (datetime.now(UTC).isoformat(), conversation_id)
        )
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return success
    
    def get_messages_for_embedding(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get messages that need embedding processing."""
        conn = self._get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(
            """SELECT * FROM fipa_messages 
               WHERE embedding_status = 'pending'
               ORDER BY timestamp
               LIMIT ?""",
            (limit,)
        )
        
        messages = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return messages
    
    def mark_embedded(self, message_id: str) -> bool:
        """Mark a message as having been processed for embeddings."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "UPDATE fipa_messages SET embedding_status = 'processed' WHERE message_id = ?",
            (message_id,)
        )
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return success
