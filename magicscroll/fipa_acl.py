"""
FIPA ACL (Agent Communication Language) implementation for MagicScroll.

This module provides an implementation of the FIPA ACL standard for agent communication
in multi-model chat systems. It serves as the foundation for multi-model communication and
messaging between different AI models and agents.

References:
- FIPA ACL: http://www.fipa.org/specs/fipa00061/SC00061G.html
"""

import sqlite3
import json
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from .config import settings

logger = logging.getLogger(__name__)

class FIPAACLMessage:
    """FIPA ACL Message implementation based on FIPA standards."""
    
    # FIPA ACL Performatives as defined in the standard
    PERFORMATIVES = [
        'ACCEPT_PROPOSAL', 'AGREE', 'CANCEL', 'CFP', 'CONFIRM',
        'DISCONFIRM', 'FAILURE', 'INFORM', 'INFORM_IF', 'INFORM_REF',
        'NOT_UNDERSTOOD', 'PROPOSE', 'QUERY_IF', 'QUERY_REF',
        'REFUSE', 'REJECT_PROPOSAL', 'REQUEST', 'REQUEST_WHEN',
        'REQUEST_WHENEVER', 'SUBSCRIBE'
    ]
    
    def __init__(self, 
                 performative: str, 
                 sender: str, 
                 receiver: Optional[str] = None, 
                 content: Optional[str] = None, 
                 conversation_id: Optional[str] = None, 
                 reply_to: Optional[str] = None, 
                 language: Optional[str] = None, 
                 encoding: Optional[str] = None, 
                 ontology: Optional[str] = None, 
                 protocol: Optional[str] = None, 
                 reply_with: Optional[str] = None, 
                 in_reply_to: Optional[str] = None, 
                 reply_by: Optional[str] = None,
                 message_id: Optional[str] = None):
        """
        Initialize a FIPA ACL message.
        
        Args:
            performative: The performative (type of communicative act)
            sender: The identity of the sender
            receiver: The identity of the intended recipient(s)
            content: The content of the message
            conversation_id: The conversation identifier
            reply_to: The identity of the agent to which replies should be sent
            language: The language in which the content is expressed
            encoding: The specific encoding of the content language expression
            ontology: The ontology(s) used to give meaning to symbols in content
            protocol: The interaction protocol used
            reply_with: An expression used by the sending agent to identify this message
            in_reply_to: The expression referenced in a previous message's reply_with
            reply_by: A time/date expression indicating when a reply should be received
            message_id: Optional ID for the message (will be generated if None)
        """
        
        if performative not in self.PERFORMATIVES:
            raise ValueError(f"Invalid performative: {performative}")
        
        self.id = message_id or str(uuid.uuid4())
        self.performative = performative
        self.sender = sender
        self.receiver = receiver
        self.content = content
        self.conversation_id = conversation_id or str(uuid.uuid4())
        self.reply_to = reply_to
        self.language = language
        self.encoding = encoding
        self.ontology = ontology
        self.protocol = protocol
        self.reply_with = reply_with
        self.in_reply_to = in_reply_to
        self.reply_by = reply_by
        self.created_at = datetime.now().isoformat()
        self.metadata = {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary using existing schema format"""
        return {
            'message_id': self.id,
            'conversation_id': self.conversation_id,
            'sender': self.sender,
            'receiver': self.receiver,
            'content': self.content,
            'performative': self.performative,
            'created_at': self.created_at,
            'reply_to': self.reply_to,
            'in_reply_to': self.in_reply_to,
            'reply_with': self.reply_with,
            'reply_by': self.reply_by if hasattr(self, 'reply_by') else None,
            'language': self.language if hasattr(self, 'language') else 'en',
            'ontology': self.ontology if hasattr(self, 'ontology') else None,
            'protocol': self.protocol if hasattr(self, 'protocol') else None,
            'conversation_state': getattr(self, 'conversation_state', None),
            'encoding': getattr(self, 'encoding', 'utf-8'),
            'content_length': len(self.content) if self.content else 0
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FIPAACLMessage':
        """Create message from dictionary using existing schema format"""
        # Extract core parameters for the constructor
        msg = cls(
            performative=data['performative'],
            sender=data['sender'],
            receiver=data.get('receiver'),
            content=data.get('content'),
            conversation_id=data.get('conversation_id'),
            reply_with=data.get('reply_with'),
            in_reply_to=data.get('in_reply_to'),
            message_id=data.get('message_id')  # Use message_id from existing schema
        )
        
        # Handle timestamp field (migration manager uses created_at)
        if 'created_at' in data:
            msg.created_at = data['created_at']
        elif 'timestamp' in data:
            msg.created_at = data['timestamp']
            
        # Handle metadata if present
        if 'metadata' in data and data['metadata']:
            try:
                msg.metadata = json.loads(data['metadata'])
            except json.JSONDecodeError:
                msg.metadata = {}
                
        return msg


class FIPAACLDatabase:
    """Database manager for FIPA ACL messages."""
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        """
        Initialize the FIPA ACL database.
        
        Args:
            db_path: Path to the SQLite database file, defaults to magicscroll sqlite path
        """
        logger.info(f"FIPAACLDatabase init: db_path={db_path}")
        
        if db_path is None:
            db_path = settings.sqlite_path
            logger.info(f"Using default SQLite path: {db_path}")
            
        self.db_path = Path(db_path)
        logger.info(f"Final db_path: {self.db_path}")
        
        # Ensure parent directory exists
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured parent directory exists: {self.db_path.parent}")
        except Exception as e:
            logger.error(f"Failed to create parent directory: {e}")
            raise
        
        # Initialize database
        try:
            logger.info(f"Connecting to SQLite database: {self.db_path}")
            self.conn = sqlite3.connect(str(self.db_path))
            logger.info("SQLite connection established")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise
            
        try:
            self._create_tables()
            logger.info("FIPA tables created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create FIPA tables: {e}")
            raise
    
    def _create_tables(self) -> None:
        """Create the necessary tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Check if tables already exist (probably created by migration manager)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('fipa_conversations', 'fipa_messages')")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        # If migration manager already created the tables, don't recreate them
        if 'fipa_conversations' in existing_tables and 'fipa_messages' in existing_tables:
            logger.info("FIPA tables already exist (created by migration manager)")
            # Just add indexes if they don't exist
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_fipa_messages_conversation
            ON fipa_messages(conversation_id)
            ''')
            
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_fipa_messages_sender
            ON fipa_messages(sender)
            ''')
            
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_fipa_messages_receiver
            ON fipa_messages(receiver)
            ''')
            
            self.conn.commit()
            return
        
        # Create tables with fallback schema (for standalone usage)
        logger.info("Creating FIPA tables with standalone schema")
        
        # Create message table - use existing schema to match claude_ingestor.py
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fipa_messages (
            message_id TEXT PRIMARY KEY,
            conversation_id TEXT,
            sender TEXT NOT NULL,
            receiver TEXT,
            speaker TEXT NOT NULL,
            content TEXT,
            performative TEXT NOT NULL,
            timestamp TEXT,
            reply_with TEXT,
            in_reply_to TEXT,
            metadata TEXT
        )
        ''')
        
        # Create conversation table - use existing schema to match claude_ingestor.py
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fipa_conversations (
            conversation_id TEXT PRIMARY KEY,
            title TEXT,
            start_time TEXT,
            end_time TEXT,
            account_uuid TEXT,
            message_count INTEGER DEFAULT 0,
            metadata TEXT
        )
        ''')
        
        # Create agents table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fipa_agents (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT,
            capabilities TEXT,
            metadata TEXT
        )
        ''')
        
        # Create index on conversation_id for faster lookups
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_fipa_messages_conversation
        ON fipa_messages(conversation_id)
        ''')
        
        # Create index on sender for faster lookups
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_fipa_messages_sender
        ON fipa_messages(sender)
        ''')
        
        # Create index on receiver for faster lookups
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_fipa_messages_receiver
        ON fipa_messages(receiver)
        ''')
        
        self.conn.commit()
    
    def save_message(self, message: FIPAACLMessage) -> None:
        """
        Save a FIPA ACL message to the database.
        
        Args:
            message: The message to save
        """
        cursor = self.conn.cursor()
        data = message.to_dict()
        
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = list(data.values())
        
        sql = f"INSERT OR REPLACE INTO fipa_messages ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, values)
        self.conn.commit()
    
    def get_message(self, message_id: str) -> Optional[FIPAACLMessage]:
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
        
        return FIPAACLMessage.from_dict(data)
    
    def get_conversation_messages(self, conversation_id: str) -> List[FIPAACLMessage]:
        """
        Retrieve all messages in a conversation.
        
        Args:
            conversation_id: The ID of the conversation
            
        Returns:
            List of messages in the conversation, ordered by timestamp
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM fipa_messages WHERE conversation_id = ? ORDER BY timestamp",
            (conversation_id,)
        )
        
        messages = []
        column_names = [description[0] for description in cursor.description]
        
        for row in cursor.fetchall():
            data = dict(zip(column_names, row))
            messages.append(FIPAACLMessage.from_dict(data))
            
        return messages
    
    def create_conversation(self, title: Optional[str] = None) -> str:
        """
        Create a new conversation.
        
        Args:
            title: An optional title for the conversation
            
        Returns:
            The ID of the newly created conversation
        """
        conversation_id = str(uuid.uuid4())
        cursor = self.conn.cursor()
        
        now = datetime.now().isoformat()
        title = title or f"Conversation {now}"
        
        cursor.execute(
            "INSERT OR REPLACE INTO fipa_conversations (conversation_id, title, start_time, end_time, account_uuid, message_count, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (conversation_id, title, now, now, '', 0, '{}')
        )
        
        self.conn.commit()
        return conversation_id
    
    def register_agent(self, 
                       name: str, 
                       agent_type: str, 
                       capabilities: Optional[Dict[str, Any]] = None, 
                       agent_id: Optional[str] = None) -> str:
        """
        Register an agent in the system.
        
        Args:
            name: The name of the agent
            agent_type: The type of agent (e.g., 'openai', 'anthropic', 'local')
            capabilities: Optional dictionary of agent capabilities
            agent_id: Optional ID for the agent (will be generated if None)
            
        Returns:
            The ID of the registered agent
        """
        agent_id = agent_id or str(uuid.uuid4())
        cursor = self.conn.cursor()
        
        capabilities_json = json.dumps(capabilities or {})
        
        cursor.execute(
            "INSERT INTO fipa_agents (id, name, type, capabilities, metadata) VALUES (?, ?, ?, ?, ?)",
            (agent_id, name, agent_type, capabilities_json, '{}')
        )
        
        self.conn.commit()
        return agent_id
    
    def get_agent(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an agent by its ID.
        
        Args:
            agent_id: The ID of the agent to retrieve
            
        Returns:
            The agent data if found, otherwise None
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM fipa_agents WHERE id = ?", (agent_id,))
        
        row = cursor.fetchone()
        if row is None:
            return None
            
        column_names = [description[0] for description in cursor.description]
        data = dict(zip(column_names, row))
        
        # Parse capabilities JSON
        if 'capabilities' in data and data['capabilities']:
            try:
                data['capabilities'] = json.loads(data['capabilities'])
            except json.JSONDecodeError:
                data['capabilities'] = {}
                
        # Parse metadata JSON
        if 'metadata' in data and data['metadata']:
            try:
                data['metadata'] = json.loads(data['metadata'])
            except json.JSONDecodeError:
                data['metadata'] = {}
                
        return data
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
    
    def __enter__(self) -> 'FIPAACLDatabase':
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


# Convenience function to get database instance
def get_fipa_acl_db() -> FIPAACLDatabase:
    """Get a FIPA ACL database instance using the configured path."""
    return FIPAACLDatabase()
