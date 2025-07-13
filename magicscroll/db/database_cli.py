"""CLI interface for database management."""

import logging
from typing import Dict

from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class DatabaseCLI:
    """Clean CLI interface for database operations."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def show_status(self):
        """Show comprehensive database status."""
        print("\n" + "="*60)
        print("📊 MAGICSCROLL DATABASE STATUS")
        print("="*60)
        
        stats = self.db_manager.get_stats()
        
        for db_name, db_stats in stats.items():
            if db_name == "migrations":
                continue
                
            print(f"\n🗄️  {db_name.upper()}:")
            
            if db_stats.get("status") == "not_exists":
                print("   ❌ Database not initialized")
            elif db_stats.get("status") == "error":
                print(f"   ❌ Error: {db_stats.get('error', 'Unknown')}")
            else:
                print(f"   ✅ Active ({db_stats.get('size_mb', 0):.1f} MB)")
                
                # Database-specific stats
                if db_name == "sqlite":
                    print(f"   📋 Conversations: {db_stats.get('conversations', 0)}")
                    print(f"   💬 Messages: {db_stats.get('messages', 0)}")
                elif db_name == "milvus":
                    print(f"   📑 Collections: {', '.join(db_stats.get('collections', []))}")
                    print(f"   🔢 MS Entries: {db_stats.get('ms_entries_count', 0)}")
                elif db_name == "kuzu":
                    print(f"   👤 Persons: {db_stats.get('persons', 0)}")
                    print(f"   🏢 Organizations: {db_stats.get('organizations', 0)}")
                    print(f"   💻 Technologies: {db_stats.get('technologies', 0)}")
                    print(f"   📝 Topics: {db_stats.get('topics', 0)}")
                    print(f"   📄 MS Entries: {db_stats.get('ms_entries', 0)}")
        
        # Migration stats
        migration_stats = stats.get("migrations", {})
        print(f"\n🔄 MIGRATIONS:")
        print(f"   📜 Total applied: {migration_stats.get('total_migrations', 0)}")
        
        by_db = migration_stats.get("by_database", {})
        for db, count in by_db.items():
            print(f"   📋 {db}: {count} migrations")
        
        print("="*60)
    
    def initialize_databases(self):
        """Initialize all database schemas."""
        print("\n🚀 Initializing MagicScroll databases...")
        
        results = self.db_manager.initialize_all()
        
        print("\n📊 Initialization Results:")
        for db, success in results.items():
            status = "✅ Success" if success else "❌ Failed"
            print(f"   {db.upper()}: {status}")
        
        if all(results.values()):
            print("\n🎉 All databases ready for use!")
        else:
            print("\n⚠️  Some databases failed to initialize. Check logs for details.")
    
    def reset_databases(self):
        """Reset all database schemas after confirmation."""
        if not self._confirm_reset():
            return
        
        print("\n♻️ Resetting database schemas...")
        
        results = self.db_manager.reset_all(confirm=True)
        
        print("\n📊 Reset Results:")
        for db, success in results.items():
            status = "✅ Success" if success else "❌ Failed"
            print(f"   {db.upper()}: {status}")
        
        if all(results.values()):
            print("\n🎉 Database schemas reset successfully!")
            print("🆕 All schemas are now clean and ready for fresh data")
        else:
            print("\n⚠️  Some database resets failed. Check logs for details.")
    
    def _confirm_reset(self) -> bool:
        """Get user confirmation for schema reset."""
        print("\n🚨 RESET DATABASE SCHEMAS")
        print("="*50)
        print("⚠️  This action will:")
        print("   - Drop ALL database tables and collections")
        print("   - Delete ALL conversation data and embeddings")
        print("   - Delete ALL entity relationships")
        print("   - Preserve database files and directories")
        print("   - Reset schemas to initial state")
        print()
        
        print("📝 Type 'CONFIRM DELETE' to proceed (case sensitive):")
        confirmation = input("> ").strip()
        
        if confirmation != "CONFIRM DELETE":
            print("❌ Reset cancelled")
            return False
        
        print(f"\n🔄 Are you absolutely sure? Type 'YES' to reset schemas:")
        final_confirm = input("> ").strip()
        
        if final_confirm != "YES":
            print("❌ Reset cancelled")
            return False
        
        return True
    
    def health_check(self) -> bool:
        """Check if all databases are healthy."""
        health = self.db_manager.health_check()
        
        print("\n🏥 Database Health Check:")
        all_healthy = True
        
        for db, is_healthy in health.items():
            status = "✅ Healthy" if is_healthy else "❌ Unhealthy"
            print(f"   {db.upper()}: {status}")
            if not is_healthy:
                all_healthy = False
        
        if all_healthy:
            print("\n🎉 All databases are healthy!")
        else:
            print("\n⚠️  Some databases need attention")
        
        return all_healthy
