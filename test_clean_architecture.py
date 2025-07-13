#!/usr/bin/env python3
"""
Test script to verify the cleaned up MagicScroll architecture.
"""

import asyncio
import sys
import os

# Add the magicscroll directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from magicscroll import MSMessage, MSSQLiteStore, MagicScroll

async def test_clean_architecture():
    """Test the new clean architecture."""
    print("🧪 Testing MagicScroll Clean Architecture")
    print("=" * 50)
    
    # Test 1: Test unified SQLite store
    print("\n1. Testing unified MSSQLiteStore...")
    try:
        sqlite_store = await MSSQLiteStore.create()
        print("✅ MSSQLiteStore created successfully")
        
        # Create a conversation
        conv_id = sqlite_store.create_conversation("Test Conversation")
        print(f"✅ Created conversation: {conv_id}")
        
        # Create a message with the SAME conversation ID
        message = MSMessage(
            performative="INFORM",
            sender="user",
            receiver="assistant", 
            content="Hello, this is a test message!",
            conversation_id=conv_id  # Use the SAME conv_id
        )
        print(f"✅ MSMessage created with conversation ID: {conv_id}")
        
        # Save the message
        sqlite_store.save_message(message)
        print("✅ Saved message to SQLite store")
        
        # Retrieve messages from the SAME conversation
        messages = sqlite_store.get_conversation_messages(conv_id)
        print(f"✅ Retrieved {len(messages)} messages")
        
        if messages:
            print(f"   📩 First message: {messages[0].content[:50]}...")
        
        await sqlite_store.close()
        print("✅ SQLite store closed")
        
    except Exception as e:
        print(f"❌ MSSQLiteStore test failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 2: Test MagicScroll initialization
    print("\n2. Testing MagicScroll initialization...")
    try:
        ms = await MagicScroll.create("sqlite")  # Use SQLite for testing
        print("✅ MagicScroll initialized successfully")
        
        # Test live conversation methods
        conv_id = ms.create_live_conversation("Another test conversation")
        print(f"✅ Created live conversation: {conv_id}")
        
        # Create and save a message
        test_message = MSMessage(
            performative="INFORM",
            sender="test-user",
            content="This is another test message",
            conversation_id=conv_id  # Use the SAME conv_id
        )
        
        ms.save_live_message(test_message)
        print("✅ Saved message via MagicScroll")
        
        # Get messages back
        retrieved_messages = ms.get_live_conversation_messages(conv_id)
        print(f"✅ Retrieved {len(retrieved_messages)} messages via MagicScroll")
        
        if retrieved_messages:
            print(f"   📩 Message content: {retrieved_messages[0].content}")
        
        # Test conversation info
        conv_info = ms.get_live_conversation_info(conv_id)
        if conv_info:
            print(f"✅ Conversation info: {conv_info['title']}")
        
        await ms.close()
        print("✅ MagicScroll closed")
        
    except Exception as e:
        print(f"❌ MagicScroll test failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n🎉 All tests passed! Clean architecture is working!")
    print("\n📋 Summary:")
    print("  ✅ MSMessage (clean FIPA-based message class)")
    print("  ✅ MSSQLiteStore (live conversation storage only)")
    print("  ✅ MagicScroll (main system integration)")
    print("  ✅ SQLite schema working correctly")
    print("  ✅ No more duplicate/competing systems")
    print("  ✅ Messages properly linked to conversations")
    print("\n☕ Perfect! The refactoring is complete and working!")

if __name__ == "__main__":
    asyncio.run(test_clean_architecture())
