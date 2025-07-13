"""Simple migration tracking manager."""

import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Optional

from ..config import settings

logger = logging.getLogger(__name__)


class MigrationManager:
    """Tracks applied database migrations."""
    
    def __init__(self):
        self.table_name = "magicscroll_migrations"
        self._ensure_tracking_table()
    
    def _ensure_tracking_table(self):
        """Ensure migration tracking table exists."""
        try:
            settings.ensure_data_dir()
            
            conn = sqlite3.connect(str(settings.sqlite_path))
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    migration_name TEXT UNIQUE NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    database_type TEXT NOT NULL,
                    success BOOLEAN NOT NULL DEFAULT 1
                )
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Could not ensure migration tracking: {e}")
    
    def is_applied(self, migration_name: str, database_type: str) -> bool:
        """Check if a migration has been applied."""
        try:
            conn = sqlite3.connect(str(settings.sqlite_path))
            cursor = conn.cursor()
            
            # Check if migration table exists first
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
            if not cursor.fetchone():
                conn.close()
                return False
            
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE migration_name = ? AND database_type = ? AND success = 1",
                (migration_name, database_type)
            )
            result = cursor.fetchone()[0] > 0
            conn.close()
            return result
        except Exception:
            return False
    
    def mark_applied(self, migration_name: str, database_type: str, success: bool = True):
        """Mark a migration as applied."""
        try:
            self._ensure_tracking_table()
            
            conn = sqlite3.connect(str(settings.sqlite_path))
            conn.execute(
                f"INSERT OR REPLACE INTO {self.table_name} (migration_name, database_type, success) VALUES (?, ?, ?)",
                (migration_name, database_type, success)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Could not mark migration applied: {e}")
    
    def clear_history(self):
        """Clear all migration history (for resets)."""
        try:
            conn = sqlite3.connect(str(settings.sqlite_path))
            conn.execute(f"DELETE FROM {self.table_name}")
            conn.commit()
            conn.close()
            logger.info("✅ Cleared migration history")
        except Exception as e:
            logger.debug(f"Could not clear migration history: {e}")
    
    def get_history(self, database_type: Optional[str] = None) -> List[Dict]:
        """Get migration history."""
        try:
            conn = sqlite3.connect(str(settings.sqlite_path))
            if database_type:
                cursor = conn.execute(
                    f"SELECT * FROM {self.table_name} WHERE database_type = ? ORDER BY applied_at",
                    (database_type,)
                )
            else:
                cursor = conn.execute(f"SELECT * FROM {self.table_name} ORDER BY applied_at")
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception:
            return []
    
    def get_history_stats(self) -> Dict:
        """Get migration history statistics."""
        try:
            migrations = self.get_history()
            return {
                "total_migrations": len(migrations),
                "by_database": {
                    db: len([m for m in migrations if m["database_type"] == db])
                    for db in ["sqlite", "milvus", "kuzu"]
                },
                "recent_migrations": migrations[-5:] if migrations else []
            }
        except Exception as e:
            return {"error": str(e)}
