#!/usr/bin/env python3
"""Test script to validate Oxigraph integration."""

import sys
from pathlib import Path

# Add magicscroll to path
sys.path.insert(0, str(Path(__file__).parent))

def test_imports():
    """Test that all required modules can be imported."""
    print("🧪 Testing Oxigraph integration...")
    
    try:
        import pyoxigraph
        print(f"✅ pyoxigraph imported successfully (version: {pyoxigraph.__version__})")
    except ImportError as e:
        print(f"❌ Failed to import pyoxigraph: {e}")
        return False
    
    try:
        from magicscroll.config import settings
        print(f"✅ Config loaded, oxigraph_path: {settings.oxigraph_path}")
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        return False
    
    try:
        from magicscroll.db.schemas import OxigraphSchema
        print("✅ OxigraphSchema imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import OxigraphSchema: {e}")
        return False
    
    try:
        from magicscroll.ms_oxigraph_store import MagicScrollOxigraphStore
        print("✅ MagicScrollOxigraphStore imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import MagicScrollOxigraphStore: {e}")
        return False
    
    try:
        from magicscroll.db import DatabaseManager
        db_manager = DatabaseManager()
        print("✅ DatabaseManager with Oxigraph support created")
    except Exception as e:
        print(f"❌ Failed to create DatabaseManager: {e}")
        return False
    
    return True

def test_oxigraph_basic():
    """Test basic Oxigraph functionality."""
    print("\n🔬 Testing basic Oxigraph functionality...")
    
    try:
        import pyoxigraph
        from pathlib import Path
        import tempfile
        import shutil
        
        # Create temporary store
        temp_dir = Path(tempfile.mkdtemp(prefix="oxigraph_test_"))
        store = pyoxigraph.Store(str(temp_dir))
        
        # Add a simple triple
        subject = pyoxigraph.NamedNode("http://example.org/subject")
        predicate = pyoxigraph.NamedNode("http://example.org/predicate")
        obj = pyoxigraph.Literal("test value")
        
        quad = pyoxigraph.Quad(subject, predicate, obj)
        store.add(quad)
        
        # Query the triple
        results = list(store.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"))
        
        if len(results) == 1:
            print("✅ Basic Oxigraph store/query functionality works")
        else:
            print(f"❌ Expected 1 result, got {len(results)}")
            return False
        
        # Clean up
        shutil.rmtree(temp_dir)
        print("✅ Temporary store cleaned up")
        
        return True
        
    except Exception as e:
        print(f"❌ Basic Oxigraph test failed: {e}")
        return False

def test_schema_creation():
    """Test that we can create the Oxigraph schema."""
    print("\n🏗️  Testing Oxigraph schema creation...")
    
    try:
        from magicscroll.db.schemas import OxigraphSchema
        from pathlib import Path
        import tempfile
        import shutil
        
        # Create temporary directory
        temp_dir = Path(tempfile.mkdtemp(prefix="oxigraph_schema_test_"))
        
        # Test schema creation
        success = OxigraphSchema.create_rdf_store(temp_dir)
        
        if success:
            print("✅ Oxigraph schema creation successful")
        else:
            print("❌ Oxigraph schema creation failed")
            return False
        
        # Test stats
        stats = OxigraphSchema.get_stats(temp_dir)
        
        if stats.get('status') == 'active':
            print(f"✅ Store stats retrieved: {stats}")
        else:
            print(f"❌ Store stats failed: {stats}")
            return False
        
        # Clean up
        shutil.rmtree(temp_dir)
        print("✅ Schema test cleanup complete")
        
        return True
        
    except Exception as e:
        print(f"❌ Schema creation test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🪄📜 MagicScroll Oxigraph Integration Test\n")
    
    all_passed = True
    
    # Test imports
    if not test_imports():
        all_passed = False
    
    # Test basic Oxigraph functionality
    if not test_oxigraph_basic():
        all_passed = False
    
    # Test schema creation
    if not test_schema_creation():
        all_passed = False
    
    print("\n" + "="*50)
    if all_passed:
        print("🎉 All tests passed! Oxigraph integration is ready.")
        print("\nNext steps:")
        print("1. Run 'magicscroll' CLI")
        print("2. Choose option 1 to drop/recreate databases (this will include Oxigraph)")
        print("3. Verify that Oxigraph shows up in the results")
    else:
        print("❌ Some tests failed. Please check the errors above.")
    
    print("="*50)

if __name__ == "__main__":
    main()
