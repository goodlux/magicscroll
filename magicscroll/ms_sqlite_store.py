import os
import sqlite3
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from pathlib import Path

from .ms_entry import MSEntry, EntryType
from .config import Config
import logging

logger = logging.getLogger(__name__)

# Default SQLite database path
DEFAULT_DB_PATH = os.path.expanduser("~/.magicscroll/magicscroll.db")

class MSSQLiteStore:
    """SQLite storage for MagicScroll."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize SQLite storage."""
        self.db_path = db_path or DEFAULT_DB_PATH
        self._ensure_directory_exists()
        
        self.conn = self._create_connection()
        
        # Initialize tables
        self._init_tables()
        
        # Set vector capabilities flag
        self.has_vec_extension = False
        
        # Set up embedding model - using sentence-transformers
        try:
            from sentence_transformers import SentenceTransformer
            self.embed_model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("Sentence transformers model loaded for embeddings")
        except ImportError:
            logger.warning("sentence-transformers not installed, vector search will be limited")
            self.embed_model = None
            
        logger.info(f"SQLite store initialized at {self.db_path}")
        if self.has_vec_extension:
            logger.info("Vector search capabilities enabled")
        else:
            logger.warning("Vector search NOT available - install with: pip install sqlite-vec")
    
    def _ensure_directory_exists(self):
        """Make sure the directory for the database exists."""
        db_dir = os.path.dirname(self.db_path)
        os.makedirs(db_dir, exist_ok=True)
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a connection to the SQLite database."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Make rows accessible by column name
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            raise
    
    def _init_tables(self):
        """Initialize database tables."""
        try:
            cursor = self.conn.cursor()
            
            # Create entries table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS entries (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                entry_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                metadata TEXT NOT NULL
            )
            ''')
            
            self.conn.commit()
            logger.info("Database tables initialized")
        except sqlite3.Error as e:
            logger.error(f"Error initializing database tables: {e}")
            self.conn.rollback()
            raise
    
    @classmethod
    async def create(cls, db_path: Optional[str] = None) -> 'MSSQLiteStore':
        """Factory method to create store instance."""
        return cls(db_path)
    
    async def save_ms_entry(self, entry: MSEntry) -> bool:
        """Store a MagicScroll entry."""
        try:
            cursor = self.conn.cursor()
            
            # Convert metadata to JSON string
            metadata_json = json.dumps(entry.metadata)
            created_at_iso = entry.created_at.isoformat()
            
            # Insert/update entry in the main table
            cursor.execute('''
            INSERT OR REPLACE INTO entries (id, content, entry_type, created_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            ''', (entry.id, entry.content, entry.entry_type.value, created_at_iso, metadata_json))
            
            self.conn.commit()
            logger.info(f"Entry {entry.id} stored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error storing entry: {e}")
            self.conn.rollback()
            return False
    
    async def get_ms_entry(self, entry_id: str) -> Optional[MSEntry]:
        """Retrieve a MagicScroll entry."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute('''
            SELECT id, content, entry_type, created_at, metadata
            FROM entries
            WHERE id = ?
            ''', (entry_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
                
            # Parse the row data
            metadata = json.loads(row['metadata'])
            
            # Create MSEntry directly
            return MSEntry(
                id=row['id'], 
                content=row['content'],
                entry_type=EntryType(row['entry_type']),
                created_at=datetime.fromisoformat(row['created_at']),
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error retrieving entry: {e}")
            return None
    
    async def delete_ms_entry(self, entry_id: str) -> bool:
        """Delete a MagicScroll entry."""
        try:
            cursor = self.conn.cursor()
            
            # Delete from main table (will cascade delete from vector table)
            cursor.execute('DELETE FROM entries WHERE id = ?', (entry_id,))
            
            self.conn.commit()
            logger.info(f"Entry {entry_id} deleted")
            return True
        except Exception as e:
            logger.error(f"Error deleting entry: {e}")
            self.conn.rollback()
            return False
    
    async def search_by_vector(
        self, 
        query_embedding: List[float], 
        limit: int = 5,
        entry_types: Optional[List[EntryType]] = None,
        temporal_filter: Optional[Dict[str, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """Return recent entries as SQLite doesn't support vector search."""
        logger.info("SQLite doesn't support vector search - falling back to returning recent entries")
        try:
            cursor = self.conn.cursor()
            
            # Build where clause for filtering
            where_clauses = []
            params = []
            
            # Add entry type filter
            if entry_types:
                placeholders = ", ".join(["?" for _ in entry_types])
                where_clauses.append(f"entry_type IN ({placeholders})")
                params.extend([t.value for t in entry_types])
            
            # Add temporal filter
            if temporal_filter:
                if 'start' in temporal_filter:
                    where_clauses.append("created_at >= ?")
                    params.append(temporal_filter['start'].isoformat())
                if 'end' in temporal_filter:
                    where_clauses.append("created_at <= ?")
                    params.append(temporal_filter['end'].isoformat())
            
            # Combine where clauses
            where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            # Fallback query - just return most recent entries
            sql = f'''
            SELECT id, content, entry_type, created_at, metadata
            FROM entries
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ?
            '''
            params.append(limit)
            cursor.execute(sql, params)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "id": row['id'],
                    "score": 0.5,  # Default score since we're not doing vector search
                    "content": row['content'],
                    "entry_type": row['entry_type'],
                    "created_at": datetime.fromisoformat(row['created_at']),
                    "metadata": json.loads(row['metadata'])
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error in vector search: {e}")
            return []
    
    async def get_recent_entries(
        self, 
        hours: Optional[int] = None,
        entry_types: Optional[List[EntryType]] = None,
        limit: int = 10
    ) -> List[MSEntry]:
        """Get recent entries from the store."""
        try:
            cursor = self.conn.cursor()
            
            # Build the query
            query = '''
            SELECT id, content, entry_type, created_at, metadata
            FROM entries
            '''
            
            # Add conditions
            conditions = []
            params = []
            
            # Time filter
            if hours is not None:
                cutoff_time = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
                conditions.append("created_at >= ?")
                params.append(cutoff_time)
            
            # Entry type filter
            if entry_types:
                type_placeholders = ", ".join(["?" for _ in entry_types])
                conditions.append(f"entry_type IN ({type_placeholders})")
                params.extend([t.value for t in entry_types])
            
            # Add WHERE clause if we have conditions
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            # Add order and limit
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            
            # Execute query
            cursor.execute(query, params)
            
            # Process results
            entries = []
            for row in cursor.fetchall():
                # Create MSEntry directly
                entries.append(MSEntry(
                    id=row['id'],
                    content=row['content'],
                    entry_type=EntryType(row['entry_type']),
                    created_at=datetime.fromisoformat(row['created_at']),
                    metadata=json.loads(row['metadata'])
                ))
            
            return entries
            
        except Exception as e:
            logger.error(f"Error getting recent entries: {e}")
            return []
    
    async def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed")

    def __del__(self):
        """Make sure connection is closed on deletion."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
