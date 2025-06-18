"""Ingest the FULL Claude export using the REAL module with proper configuration."""
import asyncio
import sys
import os
from pathlib import Path

# Add the magicscroll module to path
sys.path.append('/Users/rob/repos/magicscroll')

async def ingest_full_export():
    """Ingest the complete Claude export pipeline with REAL config."""
    
    print("🦹‍♂️ Starting FULL Claude Export Ingestion")
    
    # Import the REAL ingestor with REAL config
    from magicscroll.claude_ingestor import ClaudeMessageIngestor
    
    # Use NO arguments - force it to use the real config
    ingestor = ClaudeMessageIngestor()
    print(f"💾 Using configured database: {ingestor.db_path}")
    
    # Full export file path
    export_file = "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/conversations.json"
    
    if not os.path.exists(export_file):
        print(f"❌ Could not find full export file: {export_file}")
        return
    
    # Check file size first
    file_size = os.path.getsize(export_file)
    print(f"📂 Processing FULL export file: {export_file}")
    print(f"📊 File size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
    
    try:
        print("🚀 Starting full ingestion (this may take a while)...")
        
        # Run the complete ingestion
        result = await ingestor.ingest_claude_export(
            export_file,
            store_conversations=True,
            store_messages=True,
            create_vectors=False  # Start without vectors for speed
        )
        
        print(f"\n✅ Full ingestion completed!")
        print(f"📊 Results:")
        print(f"  - Processed conversations: {result['processed_conversations']}")
        print(f"  - Processed messages: {result['processed_messages']}")
        print(f"  - Stored to database: {result['stored_to_db']}")
        print(f"  - Errors: {result['errors']}")
        
        if result['errors'] > 0:
            print(f"\n⚠️ Error details (first 10):")
            for error in result['error_messages'][:10]:
                print(f"  - {error}")
        
        # Verify the database
        if ingestor.db_path != ':memory:' and os.path.exists(ingestor.db_path):
            import sqlite3
            conn = sqlite3.connect(ingestor.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM fipa_conversations")
            conv_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fipa_messages") 
            msg_count = cursor.fetchone()[0]
            
            # Check for attachments
            cursor.execute("SELECT COUNT(*) FROM fipa_messages WHERE metadata LIKE '%extracted_content%'")
            attachment_count = cursor.fetchone()[0]
            
            print(f"\n📊 Final database state:")
            print(f"  - Total conversations: {conv_count}")
            print(f"  - Total messages: {msg_count}")
            print(f"  - Messages with file attachments: {attachment_count}")
            
            # Show some stats
            cursor.execute("""
                SELECT speaker, COUNT(*) as count 
                FROM fipa_messages 
                GROUP BY speaker 
                ORDER BY count DESC
            """)
            speakers = cursor.fetchall()
            print(f"\n👥 Message breakdown by speaker:")
            for speaker, count in speakers:
                print(f"  - {speaker}: {count:,} messages")
            
            # Show conversation date range
            cursor.execute("""
                SELECT 
                    MIN(start_time) as earliest,
                    MAX(end_time) as latest
                FROM fipa_conversations
            """)
            date_range = cursor.fetchone()
            if date_range and date_range[0]:
                print(f"\n📅 Conversation date range:")
                print(f"  - Earliest: {date_range[0]}")
                print(f"  - Latest: {date_range[1]}")
            
            conn.close()
            
        print(f"\n🎉 Full ingestion completed successfully!")
        print(f"💾 Database location: {ingestor.db_path}")
        
        return result
        
    except Exception as e:
        print(f"❌ Full ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    asyncio.run(ingest_full_export())
