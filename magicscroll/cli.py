"""Command line interface for magicscroll."""

import typer
from rich.console import Console
from rich.table import Table

from .config import settings
from .stores import storage

app = typer.Typer(
    name="magicscroll",
    help="Multi-modal data management and retrieval system",
    add_completion=False
)
console = Console()


@app.command()
def init():
    """Initialize magicscroll data stores."""
    console.print("🪄 Initializing magicscroll...", style="bold blue")
    
    console.print(f"📁 Data directory: {settings.data_dir}")
    
    try:
        storage.init_stores()
        console.print("✅ All stores initialized successfully!", style="bold green")
    except Exception as e:
        console.print(f"❌ Error during initialization: {e}", style="bold red")
        raise typer.Exit(1)


@app.command()
def status():
    """Show status of magicscroll stores."""
    console.print("🪄 magicscroll Status", style="bold blue")
    
    table = Table(title="Storage Backends")
    table.add_column("Store", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Path", style="green")
    
    # Check if data directory exists
    data_dir_exists = settings.data_dir.exists()
    
    # Check individual stores
    oxigraph_exists = settings.oxigraph_path.exists() if settings.oxigraph_path else False
    milvus_exists = (settings.milvus_path / "milvus_lite.db").exists() if settings.milvus_path else False
    sqlite_exists = settings.sqlite_path.exists() if settings.sqlite_path else False
    
    table.add_row(
        "Data Directory", 
        "✅ Exists" if data_dir_exists else "❌ Missing",
        str(settings.data_dir)
    )
    table.add_row(
        "Oxigraph", 
        "✅ Initialized" if oxigraph_exists else "❌ Not found",
        str(settings.oxigraph_path)
    )
    table.add_row(
        "Milvus", 
        "✅ Initialized" if milvus_exists else "❌ Not found",
        str(settings.milvus_path)
    )
    table.add_row(
        "SQLite", 
        "✅ Initialized" if sqlite_exists else "❌ Not found",
        str(settings.sqlite_path)
    )
    
    console.print(table)


@app.command()
def serve(
    host: str = typer.Option(default=None, help="Host to bind to"),
    port: int = typer.Option(default=None, help="Port to bind to"),
    reload: bool = typer.Option(False, help="Enable auto-reload"),
):
    """Start the magicscroll API server."""
    import uvicorn
    
    # Use settings defaults if not provided
    host = host or settings.host
    port = port or settings.port
    
    console.print(f"🚀 Starting magicscroll server at http://{host}:{port}", style="bold green")
    
    try:
        uvicorn.run(
            "magicscroll.api:app",
            host=host,
            port=port,
            reload=reload
        )
    except ImportError:
        console.print("❌ API module not found. Create magicscroll/api.py first.", style="bold red")
        raise typer.Exit(1)


def main():
    """Main entry point."""
    app()


if __name__ == "__main__":
    main()
