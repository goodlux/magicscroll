"""Kuzu graph schema definitions for MagicScroll."""

import logging
from pathlib import Path
from typing import Dict, List

import kuzu

logger = logging.getLogger(__name__)


class KuzuSchema:
    """Kuzu graph database schema management."""
    
    @staticmethod
    def create_entity_schema(kuzu_path: Path) -> bool:
        """Create the entity-focused graph schema."""
        try:
            kuzu_path.mkdir(parents=True, exist_ok=True)
            
            db = kuzu.Database(str(kuzu_path))
            conn = kuzu.Connection(db)
            
            # Core entity node tables
            conn.execute("""
                CREATE NODE TABLE IF NOT EXISTS Person(
                    name STRING,
                    normalized_name STRING,
                    confidence DOUBLE,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    mention_count INT64,
                    PRIMARY KEY(normalized_name)
                )
            """)
            
            conn.execute("""
                CREATE NODE TABLE IF NOT EXISTS Organization(
                    name STRING,
                    normalized_name STRING,
                    confidence DOUBLE,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    mention_count INT64,
                    PRIMARY KEY(normalized_name)
                )
            """)
            
            conn.execute("""
                CREATE NODE TABLE IF NOT EXISTS Technology(
                    name STRING,
                    normalized_name STRING,
                    category STRING,
                    confidence DOUBLE,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    mention_count INT64,
                    PRIMARY KEY(normalized_name)
                )
            """)
            
            conn.execute("""
                CREATE NODE TABLE IF NOT EXISTS Topic(
                    name STRING,
                    normalized_name STRING,
                    category STRING,
                    confidence DOUBLE,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    mention_count INT64,
                    PRIMARY KEY(normalized_name)
                )
            """)
            
            conn.execute("""
                CREATE NODE TABLE IF NOT EXISTS MSEntry(
                    entry_id STRING,
                    conversation_id STRING,
                    entry_type STRING,
                    title STRING,
                    content_preview STRING,
                    created_at TIMESTAMP,
                    token_count INT64,
                    PRIMARY KEY(entry_id)
                )
            """)
            
            # Relationship tables with rich properties
            conn.execute("""
                CREATE REL TABLE IF NOT EXISTS DISCUSSED_IN(
                    FROM Person TO MSEntry,
                    confidence DOUBLE,
                    context STRING,
                    sentiment STRING,
                    mentioned_count INT64
                )
            """)
            
            conn.execute("""
                CREATE REL TABLE IF NOT EXISTS ORG_IN(
                    FROM Organization TO MSEntry,
                    confidence DOUBLE,
                    context STRING,
                    relationship_type STRING
                )
            """)
            
            conn.execute("""
                CREATE REL TABLE IF NOT EXISTS TECH_IN(
                    FROM Technology TO MSEntry,
                    confidence DOUBLE,
                    usage_context STRING,
                    proficiency_level STRING
                )
            """)
            
            conn.execute("""
                CREATE REL TABLE IF NOT EXISTS TOPIC_IN(
                    FROM Topic TO MSEntry,
                    confidence DOUBLE,
                    importance_level STRING,
                    discussion_depth STRING
                )
            """)
            
            # Entity co-occurrence relationships
            conn.execute("""
                CREATE REL TABLE IF NOT EXISTS MENTIONED_WITH(
                    FROM Person TO Person,
                    co_occurrence_count INT64,
                    last_mentioned_together TIMESTAMP,
                    relationship_context STRING
                )
            """)
            
            conn.execute("""
                CREATE REL TABLE IF NOT EXISTS WORKS_WITH(
                    FROM Person TO Organization,
                    confidence DOUBLE,
                    relationship_type STRING,
                    first_mentioned TIMESTAMP,
                    last_mentioned TIMESTAMP
                )
            """)
            
            conn.close()
            logger.info("✅ Kuzu entity schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Kuzu schema creation failed: {e}")
            return False
    
    @staticmethod
    def drop_all_data(kuzu_path: Path) -> bool:
        """Drop all data and tables from Kuzu database."""
        try:
            if not kuzu_path.exists():
                logger.info("ℹ️ Kuzu database does not exist")
                return True
                
            db = kuzu.Database(str(kuzu_path))
            conn = kuzu.Connection(db)
            
            # Drop all relationships first
            try:
                conn.execute("MATCH ()-[r]-() DELETE r")
                logger.info("✅ Deleted all Kuzu relationships")
            except:
                pass  # No relationships to delete
            
            # Drop all nodes
            try:
                conn.execute("MATCH (n) DELETE n")
                logger.info("✅ Deleted all Kuzu nodes")
            except:
                pass  # No nodes to delete
            
            # Drop all tables (more thorough cleanup)
            try:
                result = conn.execute("CALL show_tables() RETURN *")
                tables_df = result.get_as_df()
                
                if not tables_df.empty and 'name' in tables_df.columns:
                    for table_name in tables_df['name']:
                        try:
                            conn.execute(f"DROP TABLE {table_name}")
                            logger.info(f"✅ Dropped Kuzu table: {table_name}")
                        except Exception as table_error:
                            logger.debug(f"Could not drop table {table_name}: {table_error}")
            except Exception as show_error:
                logger.debug(f"Could not list Kuzu tables: {show_error}")
            
            conn.close()
            logger.info("✅ Kuzu data dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Kuzu data drop failed: {e}")
            return False
    
    @staticmethod
    def get_stats(kuzu_path: Path) -> Dict:
        """Get Kuzu database statistics."""
        try:
            if not kuzu_path.exists():
                return {"status": "not_exists", "size_mb": 0}
            
            db = kuzu.Database(str(kuzu_path))
            conn = kuzu.Connection(db)
            
            stats = {
                "status": "active",
                "size_mb": sum(f.stat().st_size for f in kuzu_path.rglob("*") if f.is_file()) / (1024*1024)
            }
            
            # Get entity counts
            entity_counts = {}
            for entity_type in ["Person", "Organization", "Technology", "Topic", "MSEntry"]:
                try:
                    result = conn.execute(f"MATCH (n:{entity_type}) RETURN count(n) as count")
                    df = result.get_as_df()
                    count = df.iloc[0]['count'] if len(df) > 0 else 0
                    entity_counts[entity_type.lower() + "s"] = count
                except:
                    entity_counts[entity_type.lower() + "s"] = 0
            
            # Legacy key mapping for compatibility
            stats.update(entity_counts)
            stats["persons"] = entity_counts.get("persons", 0)
            stats["organizations"] = entity_counts.get("organizations", 0)
            stats["technologies"] = entity_counts.get("technologys", 0)
            stats["topics"] = entity_counts.get("topics", 0)
            stats["ms_entries"] = entity_counts.get("msentrys", 0)
            
            conn.close()
            return stats
            
        except Exception as e:
            logger.debug(f"Kuzu stats error: {e}")
            return {"status": "error", "error": str(e)}
    
    @staticmethod
    def get_entity_summary(kuzu_path: Path) -> Dict:
        """Get a summary of entities and relationships."""
        try:
            db = kuzu.Database(str(kuzu_path))
            conn = kuzu.Connection(db)
            
            summary = {
                "entities": {},
                "relationships": {},
                "total_nodes": 0,
                "total_relationships": 0
            }
            
            # Count entities
            for entity_type in ["Person", "Organization", "Technology", "Topic", "MSEntry"]:
                try:
                    result = conn.execute(f"MATCH (n:{entity_type}) RETURN count(n) as count")
                    count = result.get_as_df().iloc[0]['count'] if len(result.get_as_df()) > 0 else 0
                    summary["entities"][entity_type] = count
                    summary["total_nodes"] += count
                except:
                    summary["entities"][entity_type] = 0
            
            # Count relationships
            for rel_type in ["DISCUSSED_IN", "ORG_IN", "TECH_IN", "TOPIC_IN", "MENTIONED_WITH", "WORKS_WITH"]:
                try:
                    result = conn.execute(f"MATCH ()-[r:{rel_type}]-() RETURN count(r) as count")
                    count = result.get_as_df().iloc[0]['count'] if len(result.get_as_df()) > 0 else 0
                    summary["relationships"][rel_type] = count
                    summary["total_relationships"] += count
                except:
                    summary["relationships"][rel_type] = 0
            
            conn.close()
            return summary
            
        except Exception as e:
            return {"error": str(e)}
