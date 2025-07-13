"""Oxigraph RDF store schema management."""

import logging
from pathlib import Path
from typing import Dict, Any
import pyoxigraph

logger = logging.getLogger(__name__)


class OxigraphSchema:
    """Schema management for Oxigraph RDF store."""
    
    @staticmethod
    def create_rdf_store(store_path: Path) -> bool:
        """Create or initialize Oxigraph RDF store.
        
        Args:
            store_path: Path to the store directory
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure parent directory exists
            store_path.mkdir(parents=True, exist_ok=True)
            
            # Create or open the store
            store = pyoxigraph.Store(str(store_path))
            
            # Add some basic prefixes for common vocabularies
            prefixes = [
                ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
                ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
                ("xsd", "http://www.w3.org/2001/XMLSchema#"),
                ("foaf", "http://xmlns.com/foaf/0.1/"),
                ("dc", "http://purl.org/dc/elements/1.1/"),
                ("dct", "http://purl.org/dc/terms/"),
                ("ms", "http://magicscroll.org/ontology/"),  # MagicScroll ontology
                ("fipa", "http://www.fipa.org/schemas/fipa-rdf0#"),  # FIPA-ACL ontology
            ]
            
            # Note: Oxigraph doesn't store prefixes persistently, but we can document them
            # The prefixes will need to be used in SPARQL queries when needed
            
            logger.info(f"✅ Oxigraph RDF store initialized at {store_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create Oxigraph store: {e}")
            return False
    
    @staticmethod
    def drop_all_data(store_path: Path) -> bool:
        """Drop all data from the RDF store.
        
        Args:
            store_path: Path to the store directory
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not store_path.exists():
                logger.info("Oxigraph store directory doesn't exist, nothing to drop")
                return True
            
            # Open store and clear all data
            store = pyoxigraph.Store(str(store_path))
            
            # Use SPARQL UPDATE to clear all graphs
            store.update("""
                DELETE WHERE {
                    GRAPH ?g {
                        ?s ?p ?o
                    }
                }
            """)
            
            # Also clear the default graph
            store.update("""
                DELETE WHERE {
                    ?s ?p ?o
                }
            """)
            
            logger.info(f"✅ Cleared all data from Oxigraph store at {store_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to clear Oxigraph store: {e}")
            return False
    
    @staticmethod
    def get_stats(store_path: Path) -> Dict[str, Any]:
        """Get statistics about the RDF store.
        
        Args:
            store_path: Path to the store directory
            
        Returns:
            Dict containing store statistics
        """
        try:
            if not store_path.exists():
                return {
                    "status": "not_found",
                    "path": str(store_path),
                    "error": "Store directory does not exist"
                }
            
            store = pyoxigraph.Store(str(store_path))
            
            # Count total triples
            result = store.query("""
                SELECT (COUNT(*) as ?count) WHERE {
                    ?s ?p ?o
                }
            """)
            
            total_triples = 0
            for row in result:
                # Handle pyoxigraph.Literal objects properly
                count_value = row[0]
                if hasattr(count_value, 'value'):
                    total_triples = int(count_value.value)
                else:
                    total_triples = int(str(count_value))
                break
            
            # Count triples by graph
            graph_result = store.query("""
                SELECT ?graph (COUNT(*) as ?count) WHERE {
                    GRAPH ?graph {
                        ?s ?p ?o
                    }
                } GROUP BY ?graph
            """)
            
            graphs = {}
            for row in graph_result:
                graph_uri = str(row[0])
                # Handle pyoxigraph.Literal objects properly
                count_value = row[1]
                if hasattr(count_value, 'value'):
                    count = int(count_value.value)
                else:
                    count = int(str(count_value))
                graphs[graph_uri] = count
            
            # Count default graph triples
            default_result = store.query("""
                SELECT (COUNT(*) as ?count) WHERE {
                    ?s ?p ?o
                    FILTER NOT EXISTS { GRAPH ?g { ?s ?p ?o } }
                }
            """)
            
            default_count = 0
            for row in default_result:
                # Handle pyoxigraph.Literal objects properly
                count_value = row[0]
                if hasattr(count_value, 'value'):
                    default_count = int(count_value.value)
                else:
                    default_count = int(str(count_value))
                break
            
            if default_count > 0:
                graphs["<default>"] = default_count
            
            return {
                "status": "active",
                "path": str(store_path),
                "total_triples": total_triples,
                "graphs": graphs,
                "graph_count": len(graphs)
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get Oxigraph stats: {e}")
            return {
                "status": "error",
                "path": str(store_path),
                "error": str(e)
            }
    
    @staticmethod
    def add_sample_data(store_path: Path) -> bool:
        """Add some sample RDF data for testing.
        
        Args:
            store_path: Path to the store directory
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            store = pyoxigraph.Store(str(store_path))
            
            # Add some sample triples using SPARQL INSERT
            sample_data = """
                PREFIX ms: <http://magicscroll.org/ontology/>
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                PREFIX dc: <http://purl.org/dc/elements/1.1/>
                
                INSERT DATA {
                    ms:magicscroll a ms:System ;
                        dc:title "MagicScroll" ;
                        dc:description "AI Conversation Memory & Context System" .
                    
                    ms:claude a foaf:Agent ;
                        foaf:name "Claude" ;
                        ms:systemRole "AI Assistant" .
                    
                    ms:user a foaf:Person ;
                        foaf:name "User" ;
                        ms:systemRole "Human User" .
                }
            """
            
            store.update(sample_data)
            
            logger.info("✅ Added sample RDF data to Oxigraph store")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to add sample data: {e}")
            return False
