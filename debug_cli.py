#!/usr/bin/env python3
"""Debug CLI to see what's broken with ingestion."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test all the imports the CLI needs."""
    print("🧪 Testing CLI imports...")
    
    try:
        print("1. Importing magicscroll...")
        import magicscroll
        print("✅ magicscroll imported successfully")
        
        print("2. Importing ingestor...")
        from magicscroll.ingestor import AnthropicIngestor
        print("✅ AnthropicIngestor imported successfully")
        
        print("3. Importing config...")
        from magicscroll.config import settings
        print("✅ Settings imported successfully")
        
        print("4. Importing db...")
        from magicscroll.db import DatabaseCLI
        print("✅ DatabaseCLI imported successfully")
        
        print("5. Importing MagicScroll core...")
        from magicscroll.magicscroll import MagicScroll
        print("✅ MagicScroll imported successfully")
        
        print("\n🎉 All imports successful!")
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_basic_flow():
    """Test the basic flow without running full CLI."""
    print("\n🧪 Testing basic ingestion flow...")
    
    try:
        # Test database creation
        from magicscroll.db import DatabaseCLI
        cli = DatabaseCLI()
        
        print("1. Testing database initialization...")
        results = cli.db_manager.initialize_all()
        print(f"Database init results: {results}")
        
        if not all(results.values()):
            print("❌ Database initialization failed")
            return False
        
        print("✅ Database initialization successful")
        
        # Test MagicScroll creation
        print("2. Testing MagicScroll creation...")
        import asyncio
        
        async def test_ms():
            from magicscroll.magicscroll import MagicScroll
            ms = await MagicScroll.create(storage_type="milvus")
            print("✅ MagicScroll created successfully")
            
            # Test ingestor creation
            from magicscroll.ingestor import AnthropicIngestor
            ingestor = AnthropicIngestor(magic_scroll=ms)
            print("✅ AnthropicIngestor created successfully")
            
            # Clean up
            await ms.close()
            ingestor.close()
            print("✅ Clean shutdown successful")
            
            return True
        
        result = asyncio.run(test_ms())
        return result
        
    except Exception as e:
        print(f"❌ Basic flow test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔍 Debugging MagicScroll CLI...")
    
    # Test imports first
    imports_ok = test_imports()
    
    if imports_ok:
        # Test basic flow
        flow_ok = test_basic_flow()
        
        if flow_ok:
            print("\n🎉 Everything looks good! Ingestion should work.")
            print("Try running: python -m magicscroll.cli")
        else:
            print("\n❌ Basic flow test failed. Check the error above.")
    else:
        print("\n❌ Import test failed. Fix imports first.")
