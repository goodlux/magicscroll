"""Storage backends for magicscroll with clean database integration."""

import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from pymilvus import MilvusClient
import kuzu

from .config import settings


class StorageManager:
    """Manages all storage backends for magicscroll."""
    
    def __init__(self):
        self._milvus_client: Optional[MilvusClient] = None
        self._sqlite_conn: Optional[sqlite3.Connection] = None
        self._kuzu_db: Optional[kuzu.Database] = None
        self._kuzu_conn: Optional[kuzu.Connection] = None
    
    def init_stores(self) -> None:
        """Initialize storage connections (assumes schemas already exist via database manager)."""
        settings.ensure_data_dir()
        self._init_milvus()
        self._init_sqlite()
        self._init_kuzu()
    
    def _init_milvus(self) -> None:
        """Initialize Milvus vector database connection."""
        try:
            # Using milvus-lite for local storage
            self._milvus_client = MilvusClient(str(settings.milvus_path / "milvus_lite.db"))
            print(f"✅ Milvus client connected at {settings.milvus_path}")
        except Exception as e:
            print(f"❌ Failed to connect to Milvus: {e}")
    
    def _init_sqlite(self) -> None:
        """Initialize SQLite database connection."""
        try:
            self._sqlite_conn = sqlite3.connect(str(settings.sqlite_path))
            self._sqlite_conn.execute("PRAGMA foreign_keys = ON")
            print(f"✅ SQLite database connected at {settings.sqlite_path}")
        except Exception as e:
            print(f"❌ Failed to connect to SQLite: {e}")
    
    def _init_kuzu(self) -> None:
        """Initialize Kuzu graph database connection."""
        try:
            self._kuzu_db = kuzu.Database(str(settings.kuzu_path))
            self._kuzu_conn = kuzu.Connection(self._kuzu_db)
            print(f"✅ Kuzu graph database connected at {settings.kuzu_path}")
        except Exception as e:
            print(f"❌ Failed to connect to Kuzu: {e}")
    
    @property
    def milvus(self) -> MilvusClient:
        """Get the Milvus client."""
        if self._milvus_client is None:
            raise RuntimeError("Milvus client not initialized. Run init_stores() first.")
        return self._milvus_client
    
    @property
    def sqlite(self) -> sqlite3.Connection:
        """Get the SQLite connection."""
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite connection not initialized. Run init_stores() first.")
        return self._sqlite_conn
    
    @property
    def kuzu(self) -> kuzu.Connection:
        """Get the Kuzu connection."""
        if self._kuzu_conn is None:
            raise RuntimeError("Kuzu connection not initialized. Run init_stores() first.")
        return self._kuzu_conn
    
    def close_all(self) -> None:
        """Close all storage connections."""
        if self._sqlite_conn:
            self._sqlite_conn.close()
            self._sqlite_conn = None
        if self._milvus_client:
            self._milvus_client.close()
            self._milvus_client = None
        if self._kuzu_conn:
            self._kuzu_conn.close()
            self._kuzu_conn = None
            self._kuzu_db = None
    
    def is_initialized(self) -> Dict[str, bool]:
        """Check which storage backends are initialized."""
        return {
            "sqlite": self._sqlite_conn is not None,
            "milvus": self._milvus_client is not None,
            "kuzu": self._kuzu_conn is not None
        }


# Global storage manager instance
storage = StorageManager()

