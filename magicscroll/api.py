"""FastAPI application for magicscroll."""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .stores import storage

app = FastAPI(
    title="MagicScroll API",
    description="Multi-modal data management and retrieval system",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Initialize storage backends on startup."""
    try:
        storage.init_stores()
    except Exception as e:
        # Log error but don't crash the server
        print(f"Warning: Could not initialize all stores: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up storage connections on shutdown."""
    storage.close_all()


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Welcome to MagicScroll API",
        "version": "0.1.0",
        "data_dir": str(settings.data_dir)
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/stores/status")
async def stores_status():
    """Get status of all storage backends."""
    status = {}
    
    # Check data directory
    status["data_dir"] = {
        "path": str(settings.data_dir),
        "exists": settings.data_dir.exists()
    }
    
    # Check individual stores
    status["oxigraph"] = {
        "path": str(settings.oxigraph_path),
        "exists": settings.oxigraph_path.exists() if settings.oxigraph_path else False
    }
    
    status["milvus"] = {
        "path": str(settings.milvus_path),
        "exists": (settings.milvus_path / "milvus_lite.db").exists() if settings.milvus_path else False
    }
    
    status["sqlite"] = {
        "path": str(settings.sqlite_path),
        "exists": settings.sqlite_path.exists() if settings.sqlite_path else False
    }
    
    return status


# Health check for storage backends
@app.get("/stores/ping")
async def ping_stores():
    """Ping all storage backends to check connectivity."""
    results = {}
    
    try:
        # Test Oxigraph
        store = storage.oxigraph
        results["oxigraph"] = "connected"
    except Exception as e:
        results["oxigraph"] = f"error: {str(e)}"
    
    try:
        # Test Milvus
        client = storage.milvus
        results["milvus"] = "connected"
    except Exception as e:
        results["milvus"] = f"error: {str(e)}"
    
    try:
        # Test SQLite
        conn = storage.sqlite
        conn.execute("SELECT 1")
        results["sqlite"] = "connected"
    except Exception as e:
        results["sqlite"] = f"error: {str(e)}"
    
    return results
