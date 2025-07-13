"""SQLite schema definitions for MagicScroll."""

import sqlite3
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)


class SQLiteSchema:
    """SQLite database schema management."""
    
    @staticmethod
    def create_fipa_schema(db_path: Path) -> bool:
        """Create the FIPA-ACL message schema."""
        try:
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA foreign_keys = ON")
            
            # FIPA Conversations table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fipa_conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id TEXT UNIQUE NOT NULL,
                    title TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    message_count INTEGER DEFAULT 0,
                    total_tokens INTEGER DEFAULT 0
                )
            """)
            
            # FIPA Messages table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fipa_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id TEXT NOT NULL,
                    message_id TEXT UNIQUE NOT NULL,
                    performative TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    receiver TEXT,
                    content TEXT NOT NULL,
                    language TEXT DEFAULT 'en',
                    ontology TEXT,
                    protocol TEXT,
                    conversation_state TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reply_to TEXT,
                    in_reply_to TEXT,
                    reply_with TEXT,
                    reply_by TIMESTAMP,
                    encoding TEXT DEFAULT 'utf-8',
                    content_length INTEGER,
                    FOREIGN KEY (conversation_id) REFERENCES fipa_conversations(conversation_id)
                )
            """)
            
            # Performance indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_conversation ON fipa_messages(conversation_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_created_at ON fipa_messages(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_sender ON fipa_messages(sender)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_conversations_created_at ON fipa_conversations(created_at)")
            
            conn.commit()
            conn.close()
            
            logger.info("✅ SQLite FIPA schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ SQLite schema creation failed: {e}")
            return False
    
    @staticmethod
    def drop_all_tables(db_path: Path, preserve_migration_table: str = None) -> bool:
        """Drop all data tables, optionally preserving migration tracking."""
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # Get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Drop all tables except sqlite_* system tables and migration table
            for table in tables:
                should_preserve = (
                    table.startswith('sqlite_') or 
                    (preserve_migration_table and table == preserve_migration_table)
                )
                if not should_preserve:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    logger.info(f"✅ Dropped SQLite table: {table}")
            
            # Clear migration tracking if table is preserved
            if preserve_migration_table:
                try:
                    cursor.execute(f"DELETE FROM {preserve_migration_table}")
                    logger.info("✅ Cleared migration history")
                except:
                    pass  # Migration table might not exist
            
            conn.commit()
            conn.close()
            logger.info("✅ SQLite tables dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ SQLite table drop failed: {e}")
            return False
    
    @staticmethod
    def get_stats(db_path: Path) -> Dict:
        """Get SQLite database statistics."""
        try:
            if not db_path.exists():
                return {"status": "not_exists", "size_mb": 0}
            
            conn = sqlite3.connect(str(db_path))
            stats = {"status": "active", "size_mb": db_path.stat().st_size / (1024*1024)}
            
            # Get table counts
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM fipa_conversations")
                stats["conversations"] = cursor.fetchone()[0]
            except:
                stats["conversations"] = 0
            
            try:
                cursor.execute("SELECT COUNT(*) FROM fipa_messages")
                stats["messages"] = cursor.fetchone()[0]
            except:
                stats["messages"] = 0
            
            conn.close()
            return stats
            
        except Exception as e:
            return {"status": "error", "error": str(e)}
