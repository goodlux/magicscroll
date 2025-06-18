"""Test the Claude ingestor using the REAL module with proper configuration."""
import asyncio
import sys
import os
from pathlib import Path

# Add the magicscroll module to path
sys.path.append('/Users/rob/repos/magicscroll')

async def test_real_ingestion():
    """Test the complete Claude ingestion pipeline with REAL config."""
    
    print("🦹‍♂️ Testing Complete Claude → SQLite Pipeline (REAL Config)")
    
    # Import the REAL ingestor with REAL config
    from magicscroll.claude_ingestor import ClaudeMessageIngestor
    
    # Use NO arguments - force it to use the real config
    ingestor = ClaudeMessageIngestor()
    print(f"💾 Using configured database: {ingestor.db_path}")
    
    # Test configuration - try multiple possible paths
    possible_paths = [
        "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/sample_conversations.json",
        "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/conversations.json",
        "./sample_conversations.json"
    ]
    
    export_file = None
    for path in possible_paths:
        if os.path.exists(path):
            export_file = path
            break
    
    if not export_file:
        print("❌ Could not find Claude export file. Tried:")
        for path in possible_paths:
            print(f"  - {path}")
        return
    
    try:
        print(f"📂 Processing export file: {export_file}")
        
        # Run the complete ingestion
        result = await ingestor.ingest_claude_export(
            export_file,
            store_conversations=True,
            store_messages=True,
            create_vectors=False
        )
        
        print(f"\n✅ Ingestion completed!")
        print(f"📊 Results:")
        print(f"  - Processed conversations: {result['processed_conversations']}")
        print(f"  - Processed messages: {result['processed_messages']}")
        print(f"  - Stored to database: {result['stored_to_db']}")
        print(f"  - Errors: {result['errors']}")
        
        if result['errors'] > 0:
            print(f"\n⚠️ Error details:")
            for error in result['error_messages']:
                print(f"  - {error}")
        
        # Verify the database if it exists and isn't in-memory
        if ingestor.db_path != ':memory:' and os.path.exists(ingestor.db_path):
            import sqlite3
            conn = sqlite3.connect(ingestor.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM fipa_conversations")
            conv_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fipa_messages") 
            msg_count = cursor.fetchone()[0]
            
            print(f"\n📊 Database verification:")
            print(f"  - Conversations in DB: {conv_count}")
            print(f"  - Messages in DB: {msg_count}")
            
            # Show most recent conversation
            cursor.execute("""
                SELECT conversation_id, title, message_count 
                FROM fipa_conversations 
                ORDER BY start_time DESC
                LIMIT 1
            """)
            
            sample_conv = cursor.fetchone()
            if sample_conv:
                conv_id, title, msg_count = sample_conv
                print(f"\n🔍 Most recent conversation:")
                print(f"  - ID: {conv_id}")
                print(f"  - Title: {title}")
                print(f"  - Message count: {msg_count}")
            
            conn.close()
            
        print(f"\n🎉 Test completed successfully!")
        print(f"💾 Database location: {ingestor.db_path}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    asyncio.run(test_real_ingestion())
