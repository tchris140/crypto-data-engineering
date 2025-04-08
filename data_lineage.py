import os
import json
import datetime
import uuid
import logging
import sqlite3
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DataNode:
    """Represents a data entity in the lineage graph."""
    node_id: str
    node_type: str  # 'source', 'transformation', 'destination'
    name: str
    description: str
    created_at: str
    metadata: Dict[str, Any]

@dataclass
class DataEdge:
    """Represents a relationship between data entities."""
    edge_id: str
    source_id: str
    target_id: str
    operation: str  # 'extract', 'transform', 'load', etc.
    timestamp: str
    metadata: Dict[str, Any]

class DataLineage:
    """Tracks and manages data lineage information."""
    
    def __init__(self, db_path: str = "lineage.db"):
        """Initialize the data lineage tracker.
        
        Args:
            db_path: Path to SQLite database file for storing lineage
        """
        self.db_path = db_path
        try:
            self._init_db()
        except Exception as e:
            logger.error(f"Error initializing lineage database: {e}")
            # Use in-memory database as fallback
            self.db_path = ":memory:"
            try:
                self._init_db()
                logger.info("Using in-memory database as fallback")
            except Exception as e2:
                logger.error(f"Failed to initialize in-memory database: {e2}")
    
    def _init_db(self):
        """Initialize SQLite database with proper SQLite syntax."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Create nodes table with SQLite syntax
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    node_type TEXT,
                    name TEXT,
                    description TEXT,
                    metadata TEXT
                )
            """)
            
            # Create edges table with SQLite syntax
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS edges (
                    id TEXT PRIMARY KEY,
                    source_id TEXT,
                    target_id TEXT,
                    operation TEXT,
                    metadata TEXT,
                    FOREIGN KEY (source_id) REFERENCES nodes (id),
                    FOREIGN KEY (target_id) REFERENCES nodes (id)
                )
            """)
            
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def add_node(self, node_type, name, description, metadata=None):
        """Add a node to the lineage graph with SQLite syntax."""
        try:
            node_id = str(uuid.uuid4())
            metadata_json = json.dumps(metadata) if metadata else "{}"
            
            self.cursor.execute("""
                INSERT INTO nodes (id, node_type, name, description, metadata)
                VALUES (?, ?, ?, ?, ?)
            """, (node_id, node_type, name, description, metadata_json))
            
            self.conn.commit()
            logger.info(f"Added node: {name} ({node_type})")
            return node_id
        except sqlite3.Error as e:
            logger.error(f"Error adding node: {e}")
            raise
    
    def add_edge(self, source_id, target_id, operation, metadata=None):
        """Add an edge to the lineage graph with SQLite syntax."""
        try:
            edge_id = str(uuid.uuid4())
            metadata_json = json.dumps(metadata) if metadata else "{}"
            
            self.cursor.execute("""
                INSERT INTO edges (id, source_id, target_id, operation, metadata)
                VALUES (?, ?, ?, ?, ?)
            """, (edge_id, source_id, target_id, operation, metadata_json))
            
            self.conn.commit()
            logger.info(f"Added edge: {operation} from {source_id} to {target_id}")
            return edge_id
        except sqlite3.Error as e:
            logger.error(f"Error adding edge: {e}")
            raise
    
    def get_node(self, node_id):
        """Get a node from the lineage graph with SQLite syntax."""
        try:
            self.cursor.execute("""
                SELECT id, node_type, name, description, metadata
                FROM nodes
                WHERE id = ?
            """, (node_id,))
            
            row = self.cursor.fetchone()
            if row:
                # Ensure metadata is properly initialized as a dictionary
                metadata = {}
                if row[4]:  # If metadata exists
                    try:
                        metadata = json.loads(row[4])
                        if not isinstance(metadata, dict):
                            metadata = {}
                    except json.JSONDecodeError:
                        metadata = {}
                
                return {
                    "id": row[0],
                    "node_type": row[1],
                    "name": row[2],
                    "description": row[3],
                    "metadata": metadata
                }
            return None
        except sqlite3.Error as e:
            logger.error(f"Error getting node: {e}")
            raise
    
    def get_edges(self, node_id):
        """Get edges connected to a node with SQLite syntax."""
        try:
            self.cursor.execute("""
                SELECT id, source_id, target_id, operation, metadata
                FROM edges
                WHERE source_id = ? OR target_id = ?
            """, (node_id, node_id))
            
            edges = []
            for row in self.cursor.fetchall():
                metadata = json.loads(row[4]) if row[4] else {}
                edges.append({
                    "id": row[0],
                    "source_id": row[1],
                    "target_id": row[2],
                    "operation": row[3],
                    "metadata": metadata
                })
            return edges
        except sqlite3.Error as e:
            logger.error(f"Error getting edges: {e}")
            raise
    
    def visualize(self, output_file: str = "data_lineage.html"):
        """Generate a visualization of the data lineage graph."""
        try:
            import networkx as nx
            import matplotlib.pyplot as plt
            
            G = nx.DiGraph()
            
            # Add nodes
            self.cursor.execute("SELECT id, name, node_type FROM nodes")
            for row in self.cursor.fetchall():
                G.add_node(row[0], label=row[1], type=row[2])
            
            # Add edges
            self.cursor.execute("SELECT source_id, target_id, operation FROM edges")
            for row in self.cursor.fetchall():
                G.add_edge(row[0], row[1], label=row[2])
            
            # Draw the graph
            plt.figure(figsize=(12, 8))
            pos = nx.spring_layout(G)
            nx.draw(G, pos, with_labels=True, node_color='lightblue', 
                   node_size=2000, font_size=10, font_weight='bold')
            edge_labels = nx.get_edge_attributes(G, 'label')
            nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
            
            plt.savefig(output_file)
            plt.close()
            
            logger.info(f"Data lineage visualization saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error generating visualization: {e}")
            raise
    
    def export_json(self, output_file: str = "data_lineage.json"):
        """Export the data lineage graph to JSON format."""
        try:
            lineage_data = {
                "nodes": [],
                "edges": []
            }
            
            # Get all nodes
            self.cursor.execute("SELECT * FROM nodes")
            for row in self.cursor.fetchall():
                metadata = json.loads(row[4]) if row[4] else {}
                node = {
                    "id": row[0],
                    "node_type": row[1],
                    "name": row[2],
                    "description": row[3],
                    "metadata": metadata
                }
                lineage_data["nodes"].append(node)
            
            # Get all edges
            self.cursor.execute("SELECT * FROM edges")
            for row in self.cursor.fetchall():
                metadata = json.loads(row[4]) if row[4] else {}
                edge = {
                    "id": row[0],
                    "source_id": row[1],
                    "target_id": row[2],
                    "operation": row[3],
                    "metadata": metadata
                }
                lineage_data["edges"].append(edge)
            
            # Write to file
            with open(output_file, 'w') as f:
                json.dump(lineage_data, f, indent=2)
            
            logger.info(f"Data lineage exported to {output_file}")
            
        except Exception as e:
            logger.error(f"Error exporting lineage: {e}")
            raise

# Singleton pattern for lineage tracker
_lineage_tracker = None

def get_lineage_tracker() -> DataLineage:
    """Get the global lineage tracker instance."""
    global _lineage_tracker
    if _lineage_tracker is None:
        _lineage_tracker = DataLineage()
    return _lineage_tracker

class LineageContext:
    """Context manager for tracking lineage of a data processing step."""
    
    def __init__(self, source_nodes, operation, target_name, target_description, target_type="dataset", metadata=None):
        """Initialize the lineage context.
        
        Args:
            source_nodes: List of source node IDs or a single source node ID
            operation: Operation being performed
            target_name: Name of the target node
            target_description: Description of the target node
            target_type: Type of the target node
            metadata: Additional metadata for the operation
        """
        # Ensure source_nodes is always a list and contains only valid entries
        if source_nodes is None:
            self.source_nodes = []
            logger.warning("No source nodes provided, creating orphan node")
        elif isinstance(source_nodes, list):
            # Filter out None values
            self.source_nodes = [node for node in source_nodes if node is not None]
            if len(self.source_nodes) < len(source_nodes):
                logger.warning(f"Filtered out {len(source_nodes) - len(self.source_nodes)} None values from source_nodes")
        else:
            # Single node that's not None
            self.source_nodes = [source_nodes] if source_nodes is not None else []
            
        self.operation = operation
        self.target_name = target_name
        self.target_description = target_description
        self.target_type = target_type
        self.metadata = metadata or {}
        self.target_id = None
        
        try:
            self.lineage = get_lineage_tracker()
        except Exception as e:
            logger.error(f"Error getting lineage tracker: {e}")
            # Create a new tracker instance
            self.lineage = DataLineage(db_path=":memory:")
    
    def __enter__(self):
        """Create the target node when entering the context."""
        try:
            self.target_id = self.lineage.add_node(
                node_type=self.target_type,
                name=self.target_name,
                description=self.target_description,
                metadata=self.metadata
            )
            
            # Create edges from source nodes to target
            for source_id in self.source_nodes:
                try:
                    self.lineage.add_edge(
                        source_id=source_id,
                        target_id=self.target_id,
                        operation=self.operation,
                        metadata=self.metadata
                    )
                except Exception as e:
                    logger.error(f"Error adding edge from {source_id} to {self.target_id}: {e}")
            
            return self.target_id
        except Exception as e:
            logger.error(f"Error in lineage context entry: {e}")
            # Return a dummy ID to prevent crashes
            return f"dummy_context_{str(uuid.uuid4())}"
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Handle any errors when exiting the context."""
        if exc_type is not None:
            try:
                # Log the error in metadata
                error_metadata = {
                    "error_type": str(exc_type.__name__),
                    "error_message": str(exc_val)
                }
                
                # Update the target node with error information if we have a valid target_id
                if self.target_id:
                    node = self.lineage.get_node(self.target_id)
                    if node:
                        # Ensure metadata exists and is a dictionary
                        if not isinstance(node, dict):
                            node = {"metadata": {}}
                        if "metadata" not in node:
                            node["metadata"] = {}
                        elif not isinstance(node["metadata"], dict):
                            node["metadata"] = {}
                        
                        # Update metadata with error information
                        node["metadata"].update({"error": error_metadata})
                        
                        # Update the node in the database
                        self.lineage.cursor.execute("""
                            UPDATE nodes 
                            SET metadata = ? 
                            WHERE id = ?
                        """, (json.dumps(node["metadata"]), self.target_id))
                        self.lineage.conn.commit()
                
                logger.error(f"Error in lineage context: {exc_val}")
            except Exception as e:
                logger.error(f"Error handling context exit: {e}")
        return False  # Don't suppress exceptions

if __name__ == "__main__":
    # Example usage
    lineage = get_lineage_tracker()
    
    # Add source nodes
    defi_source = lineage.add_node(
        node_type="source",
        name="DeFi Llama API",
        description="API providing Total Value Locked data for DeFi protocols",
        metadata={"url": "https://defillama.com/docs/api"}
    )
    
    reddit_source = lineage.add_node(
        node_type="source",
        name="Reddit API",
        description="API providing posts from cryptocurrency subreddits",
        metadata={"subreddit": "cryptocurrency"}
    )
    
    # Add transformation node
    transform_node = lineage.add_node(
        node_type="transformation",
        name="Data Processing",
        description="Cleanse and format data for storage",
        metadata={"transformations": ["filtering", "formatting"]}
    )
    
    # Connect source to transformation
    lineage.add_edge(
        source_id=defi_source,
        target_id=transform_node,
        operation="extract",
        metadata={"timestamp": datetime.datetime.now().isoformat()}
    )
    
    # Add destination node
    db_node = lineage.add_node(
        node_type="destination",
        name="PostgreSQL Database",
        description="Database storing cryptocurrency data",
        metadata={"database": "crypto_data"}
    )
    
    # Connect transformation to destination
    lineage.add_edge(
        source_id=transform_node,
        target_id=db_node,
        operation="load",
        metadata={"timestamp": datetime.datetime.now().isoformat()}
    )
    
    # Visualize the lineage
    lineage.visualize()
    
    # Export to JSON
    lineage.export_json() 