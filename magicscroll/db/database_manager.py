"""Clean database lifecycle management using schema modules."""

import logging
from typing import Dict
from pathlib import Path

from .schemas import SQLiteSchema, MilvusSchema, KuzuSchema
from .migration_manager import MigrationManager
from ..config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Clean database lifecycle management."""
    
    def __init__(self):
        self.migration_manager = MigrationManager()
    
    def initialize_all(self) -> Dict[str, bool]:
        """Initialize all database schemas."""
        logger.info("🚀 Initializing all MagicScroll databases...")
        
        # Ensure directories exist
        settings.ensure_data_dir()
        
        results = {
            "sqlite": self._initialize_sqlite(),
            "milvus": self._initialize_milvus(),
            "kuzu": self._initialize_kuzu()
        }
        
        if all(results.values()):
            logger.info("🎉 All databases initialized successfully!")
        else:
            failed = [db for db, success in results.items() if not success]
            logger.error(f"❌ Failed to initialize: {', '.join(failed)}")
        
        return results
    
    def _initialize_sqlite(self) -> bool:
        """Initialize SQLite database."""
        migration_name = "001_create_fipa_schema"
        
        if self.migration_manager.is_applied(migration_name, "sqlite"):
            logger.info("SQLite schema already applied")
            return True
        
        success = SQLiteSchema.create_fipa_schema(settings.sqlite_path)
        self.migration_manager.mark_applied(migration_name, "sqlite", success)
        return success
    
    def _initialize_milvus(self) -> bool:
        """Initialize Milvus collections."""
        migration_name = "001_create_ms_entries_collection"
        
        if self.migration_manager.is_applied(migration_name, "milvus"):
            logger.info("Milvus collections already applied")
            return True
        
        success = MilvusSchema.create_collections(settings.milvus_path)
        self.migration_manager.mark_applied(migration_name, "milvus", success)
        return success
    
    def _initialize_kuzu(self) -> bool:
        """Initialize Kuzu graph schema."""
        migration_name = "001_create_entity_schema"
        
        if self.migration_manager.is_applied(migration_name, "kuzu"):
            logger.info("Kuzu schema already applied")
            return True
        
        success = KuzuSchema.create_entity_schema(settings.kuzu_path)
        self.migration_manager.mark_applied(migration_name, "kuzu", success)
        return success
    
    def reset_all(self, confirm: bool = False) -> Dict[str, bool]:
        """Reset all database schemas (drop and recreate)."""
        if not confirm:
            raise ValueError("Must confirm database reset with confirm=True")
        
        logger.warning("♻️ Resetting all MagicScroll database schemas...")
        
        # Drop all schemas
        drop_results = self._drop_all_schemas()
        
        # Recreate all schemas
        create_results = self.initialize_all()
        
        # Combine results
        results = {
            db: drop_results.get(db, False) and create_results.get(db, False)
            for db in ["sqlite", "milvus", "kuzu"]
        }
        
        if all(results.values()):
            logger.info("🎉 Database schema reset completed successfully!")
        else:
            failed = [db for db, success in results.items() if not success]
            logger.error(f"❌ Database schema reset failed for: {', '.join(failed)}")
        
        return results
    
    def _drop_all_schemas(self) -> Dict[str, bool]:
        """Drop all database schemas."""
        logger.warning("🗑️ Dropping all database schemas...")
        
        results = {
            "sqlite": SQLiteSchema.drop_all_tables(
                settings.sqlite_path, 
                preserve_migration_table=self.migration_manager.table_name
            ),
            "milvus": MilvusSchema.drop_all_collections(settings.milvus_path),
            "kuzu": KuzuSchema.drop_all_data(settings.kuzu_path)
        }
        
        # Clear migration history
        self.migration_manager.clear_history()
        
        return results
    
    def get_stats(self) -> Dict[str, Dict]:
        """Get statistics for all databases."""
        return {
            "sqlite": SQLiteSchema.get_stats(settings.sqlite_path),
            "milvus": MilvusSchema.get_stats(settings.milvus_path),
            "kuzu": KuzuSchema.get_stats(settings.kuzu_path),
            "migrations": self.migration_manager.get_history_stats()
        }
    
    def health_check(self) -> Dict[str, bool]:
        """Check if all databases are healthy."""
        stats = self.get_stats()
        
        return {
            "sqlite": stats["sqlite"]["status"] == "active",
            "milvus": stats["milvus"]["status"] == "active", 
            "kuzu": stats["kuzu"]["status"] == "active"
        }
