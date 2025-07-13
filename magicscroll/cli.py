#!/usr/bin/env python3
"""MagicScroll CLI - Command line interface with clean database management."""

import os
import sys
import asyncio
import zipfile
import tempfile
import shutil
from pathlib import Path
from typing import Optional, List
import logging

# Add magicscroll to path if running directly
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, str(Path(__file__).parent))

from magicscroll.ingestor import AnthropicIngestor
from magicscroll.config import settings
from magicscroll.db import DatabaseCLI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


class MagicScrollCLI:
    """Command line interface for MagicScroll."""
    
    def __init__(self):
        """Initialize the CLI."""
        self.default_anthropic_dir = "/Users/rob/repos/anthropic"
        self.migration_cli = DatabaseCLI()
    
    def print_banner(self):
        """Print the MagicScroll banner."""
        print("=" * 60)
        print("🪄📜 Welcome to MagicScroll 🪄📜")
        print("   Your AI Conversation Memory & Context System")
        print("=" * 60)
        print()
    
    def print_menu(self):
        """Print the main menu."""
        print("🎯 Main Menu:")
        print("1) 🔄 Drop/Recreate Database")
        print("2) 📥 Ingest Anthropic Archive")
        print("3) 🚧 Ingest Other Sources (coming soon)")
        print("4) 🚪 Exit")
        print()
    
    def get_user_choice(self) -> str:
        """Get user's menu choice."""
        while True:
            try:
                choice = input("Enter your choice (1-4): ").strip()
                if choice in ['1', '2', '3', '4']:
                    return choice
                else:
                    print("❌ Invalid choice. Please enter 1-4.")
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                sys.exit(0)
            except EOFError:
                print("\n👋 Goodbye!")
                sys.exit(0)
    
    def find_latest_anthropic_archive(self, directory: str) -> Optional[Path]:
        """Find the latest Anthropic archive zip file."""
        archive_dir = Path(directory)
        
        if not archive_dir.exists():
            return None
        
        # Look for zip files
        zip_files = list(archive_dir.glob("*.zip"))
        if not zip_files:
            return None
        
        # Sort by modification time (latest first)
        zip_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        
        return zip_files[0]
    
    def confirm_file(self, file_path: Path) -> bool:
        """Ask user to confirm the file to ingest."""
        print(f"📂 Found archive: {file_path.name}")
        print(f"   Path: {file_path}")
        print(f"   Size: {file_path.stat().st_size / (1024*1024):.1f} MB")
        print(f"   Modified: {file_path.stat().st_mtime}")
        print()
        
        while True:
            try:
                confirm = input("Use this file? (y/n): ").strip().lower()
                if confirm in ['y', 'yes']:
                    return True
                elif confirm in ['n', 'no']:
                    return False
                else:
                    print("❌ Please enter 'y' or 'n'")
            except KeyboardInterrupt:
                return False
    
    def extract_conversations_json(self, zip_path: Path) -> Optional[Path]:
        """Extract conversations.json from the archive to a temp location."""
        try:
            # Create temporary directory
            temp_dir = Path(tempfile.mkdtemp(prefix="magicscroll_"))
            
            print(f"📦 Extracting archive...")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Look for conversations.json
                conversations_file = None
                for file_info in zip_ref.infolist():
                    if file_info.filename.endswith('conversations.json'):
                        conversations_file = file_info.filename
                        break
                
                if not conversations_file:
                    print("❌ No conversations.json found in archive")
                    shutil.rmtree(temp_dir)
                    return None
                
                # Extract just the conversations.json file
                zip_ref.extract(conversations_file, temp_dir)
                extracted_path = temp_dir / conversations_file
                
                print(f"✅ Extracted conversations.json ({extracted_path.stat().st_size / (1024*1024):.1f} MB)")
                return extracted_path
                
        except Exception as e:
            print(f"❌ Error extracting archive: {e}")
            if 'temp_dir' in locals():
                shutil.rmtree(temp_dir, ignore_errors=True)
            return None
    
    def get_existing_conversation_count(self) -> int:
        """Get count of existing conversations in database."""
        try:
            # Quick check using sqlite directly
            import sqlite3
            db_path = settings.sqlite_path
            
            if not db_path.exists():
                return 0
            
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM fipa_conversations")
            count = cursor.fetchone()[0]
            conn.close()
            
            return count
        except Exception:
            return 0

    def drop_recreate_database(self):
        """Drop and recreate all databases."""
        print("\n⚠️  WARNING: This will completely delete all data!")
        print("This action cannot be undone.")
        print()
        
        confirm = input("Are you sure you want to drop and recreate the database? (yes/no): ").strip().lower()
        if confirm not in ['yes', 'y']:
            print("❌ Operation cancelled")
            return
            
        print("\n🗑️  Dropping existing databases...")
        
        # Drop databases
        try:
            if settings.sqlite_path.exists():
                settings.sqlite_path.unlink()
                print(f"✅ Deleted SQLite database: {settings.sqlite_path}")
            
            if settings.milvus_path.exists():
                settings.milvus_path.unlink()
                print(f"✅ Deleted Milvus database: {settings.milvus_path}")
                
            if settings.kuzu_path.exists():
                shutil.rmtree(settings.kuzu_path)
                print(f"✅ Deleted Kuzu database: {settings.kuzu_path}")
                
        except Exception as e:
            print(f"⚠️  Error during cleanup: {e}")
        
        print("\n🔧 Recreating databases...")
        results = self.migration_cli.db_manager.initialize_all()
        
        if all(results.values()):
            print("✅ Databases recreated successfully!")
        else:
            print("❌ Some databases failed to initialize:")
            for db_name, success in results.items():
                status = "✅" if success else "❌"
                print(f"   {status} {db_name}")
    
    async def ingest_anthropic_archive(self):
        """Handle Anthropic archive ingestion with proper initialization."""
        print("\n🔍 Looking for Anthropic archives...")
        
        # First, ensure databases are initialized
        print("🔧 Ensuring databases are initialized...")
        results = self.migration_cli.db_manager.initialize_all()
        
        if not all(results.values()):
            print("❌ Database initialization failed. Please run 'Drop/Recreate Database' first.")
            return
        
        # Find latest archive
        latest_archive = self.find_latest_anthropic_archive(self.default_anthropic_dir)
        
        if not latest_archive:
            print(f"❌ No zip files found in {self.default_anthropic_dir}")
            custom_path = input("Enter custom path to archive (or press Enter to cancel): ").strip()
            if not custom_path:
                return
            latest_archive = Path(custom_path)
            if not latest_archive.exists():
                print(f"❌ File not found: {latest_archive}")
                return
        
        # Confirm file
        if not self.confirm_file(latest_archive):
            print("❌ Ingestion cancelled")
            return
        
        # Check existing data
        existing_count = self.get_existing_conversation_count()
        if existing_count > 0:
            print(f"📊 Found {existing_count} existing conversations in database")
            print("   (Duplicate conversations will be skipped)")
            print()
        
        # Extract conversations.json
        conversations_path = self.extract_conversations_json(latest_archive)
        if not conversations_path:
            return
        
        try:
            # Run ingestion with clean architecture
            print("🪄 Starting ingestion...")
            
            from magicscroll.magicscroll import MagicScroll
            
            print("🚀 Initializing MagicScroll with vector store...")
            
            magic_scroll = await MagicScroll.create(storage_type="milvus")
            
            ingestor = AnthropicIngestor(magic_scroll=magic_scroll)
            
            result = await ingestor.ingest(
                str(conversations_path),
                create_ms_entries=True,  # Enable MSEntry creation for Milvus
                limit_conversations=None  # Ingest all
            )
            
            # Clean up temp file
            temp_dir = conversations_path.parent
            shutil.rmtree(temp_dir, ignore_errors=True)
            
            # Report results
            self.print_ingestion_results(result, existing_count)
            
            # Clean shutdown
            await magic_scroll.close()
            ingestor.close()
            
        except Exception as e:
            print(f"❌ Ingestion failed: {e}")
            logger.exception("Ingestion error details:")
            
            # Clean up temp file on error
            if conversations_path and conversations_path.exists():
                temp_dir = conversations_path.parent
                shutil.rmtree(temp_dir, ignore_errors=True)
            
            # Clean up MagicScroll if it was created
            if 'magic_scroll' in locals():
                try:
                    await magic_scroll.close()
                except:
                    pass
    
    def print_ingestion_results(self, result: dict, existing_count: int):
        """Print formatted ingestion results."""
        print("\n" + "="*60)
        print("📊 INGESTION COMPLETE")
        print("="*60)
        
        if result['success']:
            print("✅ Status: SUCCESS")
        else:
            print("❌ Status: FAILED")
        
        print(f"📁 Source: {result['source']}")
        print(f"📋 Conversations processed: {result['processed_conversations']}")
        print(f"💬 Messages processed: {result['processed_messages']}")
        print(f"📄 MS Entries created: {result.get('ms_entries_created', 0)}")
        
        if result['errors'] > 0:
            print(f"⚠️  Errors: {result['errors']}")
            print("   First few errors:")
            for error in result['error_messages'][:3]:
                print(f"   - {error}")
        else:
            print("✅ No errors!")
        
        # Show database totals
        final_count = self.get_existing_conversation_count()
        new_conversations = final_count - existing_count
        
        print(f"\n📈 Database Summary:")
        print(f"   - Total conversations: {final_count}")
        print(f"   - New conversations: {new_conversations}")
        print(f"   - Existing conversations: {existing_count}")
        
        print(f"\n💾 Database location: {settings.sqlite_path}")
        print(f"🔍 Milvus location: {settings.milvus_path}")
        
        # Show comprehensive stats via migration system
        stats = self.migration_cli.db_manager.get_stats()
        
        milvus_stats = stats.get("milvus", {})
        if milvus_stats.get("status") == "active":
            print(f"🔍 Vector Search (Milvus):")
            print(f"   - MS Entries: {milvus_stats.get('ms_entries_count', 0)}")
            print(f"   - Collections: {', '.join(milvus_stats.get('collections', []))}")
        
        kuzu_stats = stats.get("kuzu", {})
        if kuzu_stats.get("status") == "active":
            print(f"🕸️  Knowledge Graph (Kuzu):")
            print(f"   - Persons: {kuzu_stats.get('persons', 0)}")
            print(f"   - Organizations: {kuzu_stats.get('organizations', 0)}")
            print(f"   - Technologies: {kuzu_stats.get('technologies', 0)}")
            print(f"   - Topics: {kuzu_stats.get('topics', 0)}")
            print(f"   - MS Entries: {kuzu_stats.get('ms_entries', 0)}")
        
        print("="*60)
    
    def handle_placeholder_option(self, option_name: str):
        """Handle placeholder menu options."""
        print(f"\n🚧 {option_name} is not yet implemented")
        print("This feature is coming soon!")
        print()
        input("Press Enter to continue...")
    
    async def run(self):
        """Run the main CLI loop."""
        self.print_banner()
        
        while True:
            self.print_menu()
            choice = self.get_user_choice()
            
            if choice == '1':
                # Drop/Recreate Database
                self.drop_recreate_database()
                input("\nPress Enter to continue...")
                
            elif choice == '2':
                # Ingest Anthropic Archive
                await self.ingest_anthropic_archive()
                input("\nPress Enter to continue...")
                
            elif choice == '3':
                # Ingest Other Sources
                self.handle_placeholder_option("Other data source ingestion")
                
            elif choice == '4':
                print("\n👋 Goodbye!")
                break
            
            print()  # Add some spacing


def main():
    """Main entry point for the CLI (synchronous wrapper for async main)."""
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        sys.exit(0)


async def async_main():
    """Async main entry point."""
    cli = MagicScrollCLI()
    await cli.run()


if __name__ == "__main__":
    main()
