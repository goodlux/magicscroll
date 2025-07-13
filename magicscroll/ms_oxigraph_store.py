"""Oxigraph RDF store wrapper for MagicScroll."""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import pyoxigraph

logger = logging.getLogger(__name__)


class MagicScrollOxigraphStore:
    """Oxigraph RDF store wrapper for MagicScroll.
    
    This class provides a convenient interface to the Oxigraph RDF store
    for storing and querying RDF data related to conversations, entities,
    and relationships.
    """
    
    def __init__(self, store_path: Union[str, Path]):
        """Initialize the Oxigraph store.
        
        Args:
            store_path: Path to the store directory
        """
        self.store_path = Path(store_path)
        self.store = None
        self._ensure_store_exists()
    
    def _ensure_store_exists(self):
        """Ensure the store directory exists and open the store."""
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.store = pyoxigraph.Store(str(self.store_path))
    
    def add_triple(self, subject: str, predicate: str, obj: str, graph: Optional[str] = None) -> bool:
        """Add a single triple to the store.
        
        Args:
            subject: Subject IRI or blank node
            predicate: Predicate IRI
            obj: Object IRI, blank node, or literal
            graph: Optional named graph IRI
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if graph:
                quad = pyoxigraph.Quad(
                    pyoxigraph.NamedNode(subject),
                    pyoxigraph.NamedNode(predicate),
                    pyoxigraph.NamedNode(obj) if obj.startswith('http') else pyoxigraph.Literal(obj),
                    pyoxigraph.NamedNode(graph)
                )
            else:
                quad = pyoxigraph.Quad(
                    pyoxigraph.NamedNode(subject),
                    pyoxigraph.NamedNode(predicate),
                    pyoxigraph.NamedNode(obj) if obj.startswith('http') else pyoxigraph.Literal(obj)
                )
            
            self.store.add(quad)
            return True
            
        except Exception as e:
            logger.error(f"Failed to add triple: {e}")
            return False
    
    def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SPARQL query.
        
        Args:
            query: SPARQL query string
            
        Returns:
            List of result dictionaries
        """
        try:
            results = []
            for result in self.store.query(query):
                if hasattr(result, '__len__') and len(result) > 0:
                    # Convert binding results to dict
                    result_dict = {}
                    for i, value in enumerate(result):
                        result_dict[f'var_{i}'] = str(value)
                    results.append(result_dict)
                else:
                    results.append({'result': str(result)})
            
            return results
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
    
    def update(self, update_query: str) -> bool:
        """Execute a SPARQL update.
        
        Args:
            update_query: SPARQL update string
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.store.update(update_query)
            return True
            
        except Exception as e:
            logger.error(f"Update failed: {e}")
            return False
    
    def add_conversation_metadata(self, conversation_id: str, title: str = None, 
                                created_at: str = None, participants: List[str] = None) -> bool:
        """Add conversation metadata as RDF triples.
        
        Args:
            conversation_id: Unique conversation identifier
            title: Optional conversation title
            created_at: Optional creation timestamp
            participants: Optional list of participant names
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Use MagicScroll ontology namespace
            ms_ns = "http://magicscroll.org/ontology/"
            conv_uri = f"{ms_ns}conversation/{conversation_id}"
            
            update_parts = [
                "PREFIX ms: <http://magicscroll.org/ontology/>",
                "PREFIX dc: <http://purl.org/dc/elements/1.1/>",
                "PREFIX dct: <http://purl.org/dc/terms/>",
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
                "INSERT DATA {"
            ]
            
            # Basic conversation metadata
            update_parts.append(f"  <{conv_uri}> a ms:Conversation .")
            
            if title:
                update_parts.append(f'  <{conv_uri}> dc:title "{title}" .')
            
            if created_at:
                update_parts.append(f'  <{conv_uri}> dct:created "{created_at}" .')
            
            if participants:
                for participant in participants:
                    participant_uri = f"{ms_ns}participant/{participant.replace(' ', '_')}"
                    update_parts.append(f"  <{conv_uri}> ms:hasParticipant <{participant_uri}> .")
                    update_parts.append(f'  <{participant_uri}> foaf:name "{participant}" .')
            
            update_parts.append("}")
            
            update_query = "\n".join(update_parts)
            return self.update(update_query)
            
        except Exception as e:
            logger.error(f"Failed to add conversation metadata: {e}")
            return False
    
    def add_entity_relationship(self, entity1: str, relationship: str, entity2: str, 
                              context: str = None) -> bool:
        """Add a relationship between two entities.
        
        Args:
            entity1: First entity
            relationship: Relationship type
            entity2: Second entity
            context: Optional context (e.g., conversation ID)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            ms_ns = "http://magicscroll.org/ontology/"
            
            # Create entity URIs
            entity1_uri = f"{ms_ns}entity/{entity1.replace(' ', '_')}"
            entity2_uri = f"{ms_ns}entity/{entity2.replace(' ', '_')}"
            rel_uri = f"{ms_ns}relation/{relationship.replace(' ', '_')}"
            
            update_parts = [
                "PREFIX ms: <http://magicscroll.org/ontology/>",
                "INSERT DATA {"
            ]
            
            update_parts.append(f"  <{entity1_uri}> <{rel_uri}> <{entity2_uri}> .")
            
            if context:
                context_uri = f"{ms_ns}context/{context}"
                statement_uri = f"{ms_ns}statement/{hash(f'{entity1}_{relationship}_{entity2}')}"
                update_parts.extend([
                    f"  <{statement_uri}> a ms:Statement .",
                    f"  <{statement_uri}> ms:subject <{entity1_uri}> .",
                    f"  <{statement_uri}> ms:predicate <{rel_uri}> .",
                    f"  <{statement_uri}> ms:object <{entity2_uri}> .",
                    f"  <{statement_uri}> ms:context <{context_uri}> ."
                ])
            
            update_parts.append("}")
            
            update_query = "\n".join(update_parts)
            return self.update(update_query)
            
        except Exception as e:
            logger.error(f"Failed to add entity relationship: {e}")
            return False
    
    def get_entity_relationships(self, entity: str) -> List[Dict[str, str]]:
        """Get all relationships for an entity.
        
        Args:
            entity: Entity name
            
        Returns:
            List of relationship dictionaries
        """
        try:
            ms_ns = "http://magicscroll.org/ontology/"
            entity_uri = f"{ms_ns}entity/{entity.replace(' ', '_')}"
            
            query = f"""
                PREFIX ms: <http://magicscroll.org/ontology/>
                
                SELECT ?relation ?target WHERE {{
                    <{entity_uri}> ?relation ?target .
                    FILTER(STRSTARTS(STR(?relation), "{ms_ns}relation/"))
                }}
                UNION
                SELECT ?relation ?target WHERE {{
                    ?target ?relation <{entity_uri}> .
                    FILTER(STRSTARTS(STR(?relation), "{ms_ns}relation/"))
                }}
            """
            
            results = self.query(query)
            relationships = []
            
            for result in results:
                if 'var_0' in result and 'var_1' in result:
                    rel_name = result['var_0'].replace(f"{ms_ns}relation/", "").replace("_", " ")
                    target = result['var_1'].replace(f"{ms_ns}entity/", "").replace("_", " ")
                    relationships.append({
                        'relationship': rel_name,
                        'target': target
                    })
            
            return relationships
            
        except Exception as e:
            logger.error(f"Failed to get entity relationships: {e}")
            return []
    
    def get_conversation_entities(self, conversation_id: str) -> List[str]:
        """Get all entities mentioned in a conversation.
        
        Args:
            conversation_id: Conversation identifier
            
        Returns:
            List of entity names
        """
        try:
            ms_ns = "http://magicscroll.org/ontology/"
            context_uri = f"{ms_ns}context/{conversation_id}"
            
            query = f"""
                PREFIX ms: <http://magicscroll.org/ontology/>
                
                SELECT DISTINCT ?entity WHERE {{
                    ?statement ms:context <{context_uri}> .
                    {{ ?statement ms:subject ?entity }}
                    UNION
                    {{ ?statement ms:object ?entity }}
                    FILTER(STRSTARTS(STR(?entity), "{ms_ns}entity/"))
                }}
            """
            
            results = self.query(query)
            entities = []
            
            for result in results:
                if 'var_0' in result:
                    entity = result['var_0'].replace(f"{ms_ns}entity/", "").replace("_", " ")
                    entities.append(entity)
            
            return entities
            
        except Exception as e:
            logger.error(f"Failed to get conversation entities: {e}")
            return []
    
    def get_stats(self) -> Dict[str, Any]:
        """Get store statistics.
        
        Returns:
            Dictionary with store statistics
        """
        try:
            # Count total triples
            total_query = """
                SELECT (COUNT(*) as ?count) WHERE {
                    ?s ?p ?o
                }
            """
            
            total_results = self.query(total_query)
            total_triples = 0
            if total_results and 'var_0' in total_results[0]:
                # The query method already converts to string, so this should work
                total_triples = int(total_results[0]['var_0'])
            
            # Count graphs
            graph_query = """
                SELECT ?graph (COUNT(*) as ?count) WHERE {
                    GRAPH ?graph {
                        ?s ?p ?o
                    }
                } GROUP BY ?graph
            """
            
            graph_results = self.query(graph_query)
            graphs = {}
            for result in graph_results:
                if 'var_0' in result and 'var_1' in result:
                    graph_uri = result['var_0']
                    count = int(result['var_1'])
                    graphs[graph_uri] = count
            
            return {
                'status': 'active',
                'path': str(self.store_path),
                'total_triples': total_triples,
                'graphs': graphs,
                'graph_count': len(graphs)
            }
            
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {
                'status': 'error',
                'path': str(self.store_path),
                'error': str(e)
            }
    
    def close(self):
        """Close the store (cleanup if needed)."""
        # Oxigraph handles cleanup automatically
        self.store = None
