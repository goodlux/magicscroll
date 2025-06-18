"""Test the complete Claude ingestor with SQLite storage."""
import asyncio
import sys
import os
from pathlib import Path

# Add the magicscroll module to path
sys.path.append('/Users/rob/repos/magicscroll')

from magicscroll.claude_ingestor import ClaudeMessageIngestor

async def test_full_ingestion():
    """Test the complete Claude ingestion pipeline with SQLite storage."""
    
    print("🦹‍♂️ Testing Complete Claude → SQLite Pipeline")
    
    # Test configuration
    export_file = "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/sample_conversations.json"
    test_db = "/tmp/test_magicscroll_full.db"
    
    # Clean up previous test db
    if os.path.exists(test_db):
        os.remove(test_db)
        print(f"🗑️  Cleaned up previous test database")
    
    # Initialize ingestor
    ingestor = ClaudeMessageIngestor(test_db)
    
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
        
        # Verify the database
        if os.path.exists(test_db):
            import sqlite3
            conn = sqlite3.connect(test_db)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM fipa_conversations")
            conv_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM fipa_messages") 
            msg_count = cursor.fetchone()[0]
            
            print(f"\n📊 Database verification:")
            print(f"  - Conversations in DB: {conv_count}")
            print(f"  - Messages in DB: {msg_count}")
            
            # Show a sample conversation
            cursor.execute("""
                SELECT conversation_id, title, message_count 
                FROM fipa_conversations 
                LIMIT 1
            """)
            
            sample_conv = cursor.fetchone()
            if sample_conv:
                conv_id, title, msg_count = sample_conv
                print(f"\n🔍 Sample conversation:")
                print(f"  - ID: {conv_id}")
                print(f"  - Title: {title}")
                print(f"  - Message count: {msg_count}")
                
                # Show sample messages
                cursor.execute("""
                    SELECT speaker, performative, content
                    FROM fipa_messages 
                    WHERE conversation_id = ?
                    ORDER BY timestamp
                    LIMIT 3
                """, (conv_id,))
                
                messages = cursor.fetchall()
                print(f"  - Sample messages:")
                for i, (speaker, perf, content) in enumerate(messages):
                    preview = content[:60] + "..." if len(content) > 60 else content
                    print(f"    {i+1}. {speaker} ({perf}): {preview}")
            
            conn.close()
            
        print(f"\n🎉 Test completed successfully!")
        print(f"💾 Database saved to: {test_db}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_full_ingestion())
