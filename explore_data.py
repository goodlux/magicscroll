#!/usr/bin/env python3
"""Explore what's actually in the MagicScroll SQLite database."""

import sys
import sqlite3
import json
from pathlib import Path

# Add magicscroll to path
sys.path.insert(0, str(Path(__file__).parent))

from magicscroll.config import settings

def explore_database():
    """Look at what we actually have in the database."""
    print("🔍 Exploring MagicScroll Database Contents")
    print("=" * 50)
    
    if not settings.sqlite_path.exists():
        print("❌ No database found. Run ingestion first!")
        return
    
    conn = sqlite3.connect(str(settings.sqlite_path))
    cursor = conn.cursor()
    
    # Check what tables we have
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"📋 Tables found: {[t[0] for t in tables]}")
    print()
    
    # Look at conversations
    print("💬 CONVERSATIONS:")
    cursor.execute("SELECT COUNT(*) FROM fipa_conversations")
    conv_count = cursor.fetchone()[0]
    print(f"   Total conversations: {conv_count}")
    
    if conv_count > 0:
        cursor.execute("""
            SELECT conversation_id, title, created_at, message_count 
            FROM fipa_conversations 
            LIMIT 5
        """)
        conversations = cursor.fetchall()
        
        print("   Sample conversations:")
        for conv in conversations:
            conv_id, title, created_at, msg_count = conv
            title_short = (title[:50] + "...") if title and len(title) > 50 else title
            print(f"     • {conv_id}: {title_short} ({msg_count} messages)")
    
    print()
    
    # Look at messages
    print("📝 MESSAGES:")
    cursor.execute("SELECT COUNT(*) FROM fipa_messages")
    msg_count = cursor.fetchone()[0]
    print(f"   Total messages: {msg_count}")
    
    if msg_count > 0:
        cursor.execute("""
            SELECT conversation_id, role, content_type, LENGTH(content)
            FROM fipa_messages 
            LIMIT 10
        """)
        messages = cursor.fetchall()
        
        print("   Sample messages:")
        for msg in messages:
            conv_id, role, content_type, content_len = msg
            print(f"     • {conv_id}: {role} ({content_type}, {content_len} chars)")
    
    print()
    
    # Look for documents/artifacts
    print("📄 LOOKING FOR DOCUMENTS/ARTIFACTS:")
    cursor.execute("""
        SELECT COUNT(*) FROM fipa_messages 
        WHERE content LIKE '%artifact%' OR content LIKE '%document%' OR content LIKE '%pdf%'
    """)
    artifact_count = cursor.fetchone()[0]
    print(f"   Messages mentioning artifacts/docs: {artifact_count}")
    
    if artifact_count > 0:
        cursor.execute("""
            SELECT conversation_id, role, content
            FROM fipa_messages 
            WHERE content LIKE '%artifact%' OR content LIKE '%document%' OR content LIKE '%pdf%'
            LIMIT 3
        """)
        artifact_messages = cursor.fetchall()
        
        print("   Sample artifact-related messages:")
        for msg in artifact_messages:
            conv_id, role, content = msg
            content_snippet = content[:200] + "..." if len(content) > 200 else content
            print(f"     • {conv_id} ({role}): {content_snippet}")
    
    print()
    
    # Look at actual message content structure
    print("🔬 MESSAGE CONTENT ANALYSIS:")
    cursor.execute("""
        SELECT content FROM fipa_messages 
        WHERE LENGTH(content) > 100
        LIMIT 3
    """)
    sample_contents = cursor.fetchall()
    
    for i, (content,) in enumerate(sample_contents, 1):
        print(f"   Sample message {i} (first 300 chars):")
        print(f"     {content[:300]}...")
        
        # Try to detect if it's JSON
        try:
            parsed = json.loads(content)
            print(f"     → This is JSON with keys: {list(parsed.keys())}")
        except:
            print(f"     → This is plain text")
        print()
    
    conn.close()
    print("✅ Database exploration complete!")

if __name__ == "__main__":
    explore_database()
