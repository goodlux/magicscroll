"""Storage backends for magicscroll."""

import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from pymilvus import MilvusClient
import pyoxigraph

from .config import settings


class StorageManager:
    """Manages all storage backends for magicscroll."""
    
    def __init__(self):
        self._oxigraph_store: Optional[pyoxigraph.Store] = None
        self._milvus_client: Optional[MilvusClient] = None
        self._sqlite_conn: Optional[sqlite3.Connection] = None
    
    def init_stores(self) -> None:
        """Initialize all storage backends."""
        settings.ensure_data_dir()
        self._init_oxigraph()
        self._init_milvus()
        self._init_sqlite()
    
    def _init_oxigraph(self) -> None:
        """Initialize Oxigraph RDF store."""
        try:
            self._oxigraph_store = pyoxigraph.Store(str(settings.oxigraph_path))
            print(f"✅ Oxigraph store initialized at {settings.oxigraph_path}")
        except Exception as e:
            print(f"❌ Failed to initialize Oxigraph: {e}")
    
    def _init_milvus(self) -> None:
        """Initialize Milvus vector database."""
        try:
            # Using milvus-lite for local storage
            self._milvus_client = MilvusClient(str(settings.milvus_path / "milvus_lite.db"))
            print(f"✅ Milvus client initialized at {settings.milvus_path}")
        except Exception as e:
            print(f"❌ Failed to initialize Milvus: {e}")
    
    def _init_sqlite(self) -> None:
        """Initialize SQLite database."""
        try:
            self._sqlite_conn = sqlite3.connect(str(settings.sqlite_path))
            self._sqlite_conn.execute("PRAGMA foreign_keys = ON")
            print(f"✅ SQLite database initialized at {settings.sqlite_path}")
        except Exception as e:
            print(f"❌ Failed to initialize SQLite: {e}")
    
    @property
    def oxigraph(self) -> pyoxigraph.Store:
        """Get the Oxigraph store."""
        if self._oxigraph_store is None:
            raise RuntimeError("Oxigraph store not initialized")
        return self._oxigraph_store
    
    @property
    def milvus(self) -> MilvusClient:
        """Get the Milvus client."""
        if self._milvus_client is None:
            raise RuntimeError("Milvus client not initialized")
        return self._milvus_client
    
    @property
    def sqlite(self) -> sqlite3.Connection:
        """Get the SQLite connection."""
        if self._sqlite_conn is None:
            raise RuntimeError("SQLite connection not initialized")
        return self._sqlite_conn
    
    def close_all(self) -> None:
        """Close all storage connections."""
        if self._sqlite_conn:
            self._sqlite_conn.close()
        if self._milvus_client:
            self._milvus_client.close()
        # pyoxigraph Store automatically closes when garbage collected


# Global storage manager instance
storage = StorageManager()
