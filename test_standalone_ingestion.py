"""Standalone test for Claude ingestor without module dependencies."""
import asyncio
import sys
import os
import json
import sqlite3
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClaudeMessageIngestor:
    """Standalone version for testing - copied from claude_ingestor.py"""
    
    def __init__(self, db_path: str = None):
        """Initialize the Claude message ingestor."""
        if db_path is None:
            # Use the configured magicscroll database path
            db_path = "/Users/rob/.magicscroll/sqlite/magicscroll-sqlite.db"
            # Ensure directory exists
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.db_path = db_path
        self.processed_conversations = 0
        self.processed_messages = 0
        self.errors = []
        self.connection = None
    
    def connect_db(self) -> sqlite3.Connection:
        """Connect to SQLite database and return connection."""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
        return self.connection
    
    def close_db(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def ensure_database_schema(self):
        """Ensure the database has the required tables."""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        # Create conversations table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fipa_conversations (
                conversation_id TEXT PRIMARY KEY,
                title TEXT,
                start_time TEXT,
                end_time TEXT,
                account_uuid TEXT,
                message_count INTEGER DEFAULT 0,
                metadata TEXT
            )
        """)
        
        # Create messages table
        cursor.execute("""
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
                metadata TEXT,
                FOREIGN KEY (conversation_id) REFERENCES fipa_conversations (conversation_id)
            )
        """)
        
        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_conversation ON fipa_messages(conversation_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON fipa_messages(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_speaker ON fipa_messages(speaker)")
        
        conn.commit()
        logger.info("Database schema ensured")
    
    def parse_claude_export(self, export_path: str) -> List[Dict[str, Any]]:
        """Parse Claude export JSON file."""
        try:
            with open(export_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                raise ValueError("Expected list of conversations at top level")
                
            logger.info(f"Loaded {len(data)} conversations from {export_path}")
            return data
            
        except Exception as e:
            logger.error(f"Error parsing Claude export: {e}")
            raise
    
    def extract_message_content(self, message: Dict[str, Any]) -> str:
        """Extract the main text content from a Claude message."""
        # Try text field first
        if 'text' in message and message['text']:
            return message['text']
        
        # Fallback to content array
        if 'content' in message and isinstance(message['content'], list):
            text_parts = []
            for content_block in message['content']:
                if content_block.get('type') == 'text' and content_block.get('text'):
                    text_parts.append(content_block['text'])
            
            if text_parts:
                return '\n'.join(text_parts)
        
        return ""
    
    def extract_message_metadata(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from Claude message."""
        metadata = {
            'original_format': 'claude_export',
            'claude_message_uuid': message.get('uuid', ''),
            'updated_at': message.get('updated_at', ''),
        }
        
        # Add attachment information
        if 'attachments' in message and message['attachments']:
            metadata['attachments'] = message['attachments']
        
        # Add file information
        if 'files' in message and message['files']:
            metadata['files'] = message['files']
        
        # Add content structure if complex
        if 'content' in message and isinstance(message['content'], list):
            metadata['content_structure'] = message['content']
        
        return metadata
    
    def convert_to_fipa_message(
        self, 
        claude_message: Dict[str, Any], 
        conversation_id: str,
        previous_message_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Convert Claude message to FIPA-ACL format."""
        # Get the actual speaker from Claude export
        speaker = claude_message.get('sender', 'unknown')
        
        # Map to FIPA performatives while preserving speaker identity
        if speaker == 'human':
            performative = 'REQUEST'
            sender_id = 'user'
            receiver_id = 'assistant'
        elif speaker == 'assistant':
            performative = 'INFORM'
            sender_id = 'assistant'
            receiver_id = 'user'
        else:
            performative = 'INFORM'
            sender_id = speaker
            receiver_id = None
        
        # Extract content and metadata
        content = self.extract_message_content(claude_message)
        metadata = self.extract_message_metadata(claude_message)
        
        # Create simplified message dict for storage
        fipa_message = {
            'message_id': claude_message.get('uuid', str(uuid.uuid4())),
            'conversation_id': conversation_id,
            'sender': sender_id,
            'receiver': receiver_id,
            'speaker': speaker,  # Actual speaker identity
            'content': content,
            'performative': performative,
            'timestamp': claude_message.get('created_at', ''),
            'reply_with': None,
            'in_reply_to': previous_message_id,
            'metadata': json.dumps(metadata)
        }
        
        return fipa_message
    
    def process_conversation(self, conversation: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a single Claude conversation into FIPA-ACL messages."""
        try:
            conversation_id = conversation.get('uuid', str(uuid.uuid4()))
            messages = conversation.get('chat_messages', [])
            
            fipa_messages = []
            previous_message_id = None
            
            # Sort messages by created_at to ensure proper order
            sorted_messages = sorted(
                messages, 
                key=lambda m: m.get('created_at', '1970-01-01T00:00:00Z')
            )
            
            for claude_message in sorted_messages:
                try:
                    fipa_message = self.convert_to_fipa_message(
                        claude_message, 
                        conversation_id,
                        previous_message_id
                    )
                    fipa_messages.append(fipa_message)
                    previous_message_id = fipa_message['message_id']
                    
                except Exception as e:
                    self.errors.append(f"Error processing message {claude_message.get('uuid', 'unknown')}: {e}")
                    logger.warning(f"Skipping message due to error: {e}")
                    continue
            
            self.processed_messages += len(fipa_messages)
            return fipa_messages
            
        except Exception as e:
            self.errors.append(f"Error processing conversation {conversation.get('uuid', 'unknown')}: {e}")
            logger.error(f"Error processing conversation: {e}")
            return []
    
    def create_conversation_record(self, conversation: Dict[str, Any]) -> Dict[str, Any]:
        """Create conversation record for storage."""
        return {
            'conversation_id': conversation.get('uuid', str(uuid.uuid4())),
            'title': conversation.get('name', 'Untitled Conversation'),
            'start_time': conversation.get('created_at', ''),
            'end_time': conversation.get('updated_at', ''),
            'account_uuid': conversation.get('account', {}).get('uuid', ''),
            'message_count': len(conversation.get('chat_messages', [])),
            'metadata': json.dumps({
                'original_format': 'claude_export',
                'has_attachments': any(
                    msg.get('attachments') or msg.get('files') 
                    for msg in conversation.get('chat_messages', [])
                )
            })
        }
    
    def store_conversation(self, conversation_record: Dict[str, Any]) -> bool:
        """Store a conversation record in the database."""
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO fipa_conversations (
                    conversation_id, title, start_time, end_time, 
                    account_uuid, message_count, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                conversation_record['conversation_id'],
                conversation_record['title'],
                conversation_record['start_time'],
                conversation_record['end_time'],
                conversation_record['account_uuid'],
                conversation_record['message_count'],
                conversation_record['metadata']
            ))
            
            conn.commit()
            logger.info(f"Stored conversation: {conversation_record['conversation_id']}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing conversation: {e}")
            return False
    
    def store_message(self, message_record: Dict[str, Any]) -> bool:
        """Store a message record in the database."""
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO fipa_messages (
                    message_id, conversation_id, sender, receiver, speaker,
                    content, performative, timestamp, reply_with, 
                    in_reply_to, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                message_record['message_id'],
                message_record['conversation_id'],
                message_record['sender'],
                message_record['receiver'], 
                message_record['speaker'],
                message_record['content'],
                message_record['performative'],
                message_record['timestamp'],
                message_record['reply_with'],
                message_record['in_reply_to'],
                message_record['metadata']
            ))
            
            conn.commit()
            logger.info(f"Stored message: {message_record['message_id']}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing message: {e}")
            return False
    
    async def ingest_claude_export(
        self, 
        export_path: str,
        store_conversations: bool = True,
        store_messages: bool = True,
        create_vectors: bool = True
    ) -> Dict[str, Any]:
        """Main ingestion method."""
        logger.info(f"Starting Claude export ingestion from {export_path}")
        
        # Reset counters
        self.processed_conversations = 0
        self.processed_messages = 0
        self.errors = []
        
        # Ensure database schema if we're storing data
        if store_conversations or store_messages:
            self.ensure_database_schema()
        
        try:
            # Parse the export file
            conversations = self.parse_claude_export(export_path)
            
            total_fipa_messages = []
            conversation_summaries = []
            
            # Process each conversation
            for conversation in conversations:
                try:
                    # Convert to FIPA messages and store them
                    fipa_messages = self.process_conversation(conversation)
                    if fipa_messages and store_messages:
                        stored_messages = []
                        for msg in fipa_messages:
                            if self.store_message(msg):
                                stored_messages.append(msg)
                            else:
                                self.errors.append(f"Failed to store message {msg.get('message_id', 'unknown')}")
                        total_fipa_messages.extend(stored_messages)
                    elif fipa_messages:
                        total_fipa_messages.extend(fipa_messages)
                    
                    # Create and store conversation record
                    if store_conversations:
                        conv_record = self.create_conversation_record(conversation)
                        if self.store_conversation(conv_record):
                            conversation_summaries.append(conv_record)
                        else:
                            self.errors.append(f"Failed to store conversation {conversation.get('uuid', 'unknown')}")
                    
                    self.processed_conversations += 1
                    
                    if self.processed_conversations % 100 == 0:
                        logger.info(f"Processed {self.processed_conversations} conversations...")
                        
                except Exception as e:
                    self.errors.append(f"Failed to process conversation: {e}")
                    logger.warning(f"Skipping conversation due to error: {e}")
                    continue
            
            # Close database connection
            self.close_db()
            
            summary = {
                'processed_conversations': self.processed_conversations,
                'processed_messages': self.processed_messages,
                'total_fipa_messages': len(total_fipa_messages),
                'conversation_summaries': len(conversation_summaries),
                'errors': len(self.errors),
                'error_messages': self.errors[:10],  # First 10 errors
                'stored_to_db': store_conversations or store_messages
            }
            
            logger.info(f"Ingestion complete: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Fatal error during ingestion: {e}")
            raise


async def test_full_ingestion():
    """Test the complete Claude ingestion pipeline with configured database."""
    
    print("🦹‍♂️ Testing Complete Claude → SQLite Pipeline (Using Configured Database)")
    
    # Use default configured path (no hardcoded paths!)
    ingestor = ClaudeMessageIngestor()  # Will use settings.sqlite_path
    print(f"💾 Using configured database: {ingestor.db_path}")
    
    # Test configuration - try multiple possible paths
    possible_paths = [
        "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/sample_conversations.json",
        "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/conversations.json",
        "./sample_conversations.json"
    ]
    
    export_file = None
    for path in possible_paths:
        if os.path.exists(path):
            export_file = path
            break
    
    if not export_file:
        print("❌ Could not find Claude export file. Tried:")
        for path in possible_paths:
            print(f"  - {path}")
        return
    
    try:
        print(f"📂 Processing export file: {export_file}")
        
        # Run the complete ingestion
        result = await ingestor.ingest_claude_export(
            export_file,
            store_conversations=True,
            store_messages=True,
            create_vectors=False
        )
        
        print(f"\n✅ Ingestion completed!")
        print(f"📊 Results:")
        print(f"  - Processed conversations: {result['processed_conversations']}")
        print(f"  - Processed messages: {result['processed_messages']}")
        print(f"  - Stored to database: {result['stored_to_db']}")
        print(f"  - Errors: {result['errors']}")
        
        if result['errors'] > 0:
            print(f"\n⚠️ Error details:")
            for error in result['error_messages']:
                print(f"  - {error}")
        
        # Verify the database
        if os.path.exists(ingestor.db_path):
            conn = sqlite3.connect(ingestor.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM fipa_conversations")
            conv_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fipa_messages") 
            msg_count = cursor.fetchone()[0]
            
            print(f"\n📊 Database verification (REAL database):")
            print(f"  - Conversations in DB: {conv_count}")
            print(f"  - Messages in DB: {msg_count}")
            
            # Show a sample conversation
            cursor.execute("""
                SELECT conversation_id, title, message_count 
                FROM fipa_conversations 
                ORDER BY start_time DESC
                LIMIT 1
            """)
            
            sample_conv = cursor.fetchone()
            if sample_conv:
                conv_id, title, msg_count = sample_conv
                print(f"\n🔍 Most recent conversation:")
                print(f"  - ID: {conv_id}")
                print(f"  - Title: {title}")
                print(f"  - Message count: {msg_count}")
                
                # Show sample messages
                cursor.execute("""
                    SELECT speaker, performative, content
                    FROM fipa_messages 
                    WHERE conversation_id = ?
                    ORDER BY timestamp
                    LIMIT 3
                """, (conv_id,))
                
                messages = cursor.fetchall()
                print(f"  - Sample messages:")
                for i, (speaker, perf, content) in enumerate(messages):
                    preview = content[:60] + "..." if len(content) > 60 else content
                    print(f"    {i+1}. {speaker} ({perf}): {preview}")
            
            conn.close()
            
        print(f"\n🎉 Test completed successfully!")
        print(f"💾 All data stored in configured database: {ingestor.db_path}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_full_ingestion())
