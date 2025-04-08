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
        """Initialize the SQLite database schema if it doesn't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create nodes table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    node_type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at TEXT NOT NULL,
                    metadata TEXT
                )
                ''')
                
                # Create edges table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS edges (
                    edge_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    metadata TEXT,
                    FOREIGN KEY (source_id) REFERENCES nodes (node_id),
                    FOREIGN KEY (target_id) REFERENCES nodes (node_id)
                )
                ''')
                
                conn.commit()
                logger.info(f"Initialized data lineage database at {self.db_path}")
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise
    
    def add_node(self, node_type: str, name: str, description: str = "",
                 metadata: Optional[Dict[str, Any]] = None) -> str:
        """Add a node to the lineage graph.
        
        Args:
            node_type: Type of node ('source', 'transformation', 'destination', etc.)
            name: Name of the node
            description: Description of the node
            metadata: Additional metadata about the node
            
        Returns:
            node_id: ID of the created node
        """
        try:
            node_id = str(uuid.uuid4())
            created_at = datetime.datetime.now().isoformat()
            metadata = metadata or {}
            
            node = DataNode(
                node_id=node_id,
                node_type=node_type,
                name=name,
                description=description,
                created_at=created_at,
                metadata=metadata
            )
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?)",
                    (node.node_id, node.node_type, node.name, node.description,
                     node.created_at, json.dumps(node.metadata))
                )
                conn.commit()
                
            logger.info(f"Added node: {name} ({node_type})")
            return node_id
        except Exception as e:
            logger.error(f"Error adding node: {e}")
            # Return a dummy node ID to prevent crashes
            dummy_id = f"dummy_{str(uuid.uuid4())}"
            logger.info(f"Returning dummy node ID: {dummy_id}")
            return dummy_id
    
    def add_edge(self, source_id: str, target_id: str, operation: str,
                metadata: Optional[Dict[str, Any]] = None) -> str:
        """Add an edge between two nodes in the lineage graph.
        
        Args:
            source_id: ID of the source node
            target_id: ID of the target node
            operation: Type of operation ('extract', 'transform', 'load', etc.)
            metadata: Additional metadata about the operation
            
        Returns:
            edge_id: ID of the created edge
        """
        try:
            cursor = self.conn.cursor()
            
            # Check if edge already exists
            cursor.execute("""
                SELECT 1 FROM edges 
                WHERE source_id = ? AND target_id = ? AND operation = ?
            """, (source_id, target_id, operation))
            
            if cursor.fetchone():
                logger.info(f"Edge already exists: {operation} from {source_id} to {target_id}")
                return
            
            # Add edge with proper SQLite syntax
            cursor.execute("""
                INSERT INTO edges (source_id, target_id, operation, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (source_id, target_id, operation))
            
            self.conn.commit()
            logger.info(f"Added edge: {operation} from {source_id} to {target_id}")
            
        except sqlite3.Error as e:
            logger.error(f"Error adding edge: {e}")
            raise
    
    def get_node(self, node_id: str) -> Optional[DataNode]:
        """Get a node by its ID.
        
        Args:
            node_id: ID of the node to retrieve
            
        Returns:
            DataNode or None if not found
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
                row = cursor.fetchone()
                
                if row:
                    try:
                        metadata = json.loads(row['metadata'])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON metadata for node {node_id}, using empty dict")
                        metadata = {}
                        
                    return DataNode(
                        node_id=row['node_id'],
                        node_type=row['node_type'],
                        name=row['name'],
                        description=row['description'],
                        created_at=row['created_at'],
                        metadata=metadata
                    )
                return None
        except Exception as e:
            logger.error(f"Error getting node {node_id}: {e}")
            # Return a dummy node to prevent crashes
            return DataNode(
                node_id=node_id,
                node_type="unknown",
                name="Error retrieving node",
                description="An error occurred while retrieving this node",
                created_at=datetime.datetime.now().isoformat(),
                metadata={"error": str(e)}
            )
    
    def get_edge(self, edge_id: str) -> Optional[DataEdge]:
        """Get an edge by its ID.
        
        Args:
            edge_id: ID of the edge to retrieve
            
        Returns:
            DataEdge or None if not found
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM edges WHERE edge_id = ?", (edge_id,))
                row = cursor.fetchone()
                
                if row:
                    try:
                        metadata = json.loads(row['metadata'])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON metadata for edge {edge_id}, using empty dict")
                        metadata = {}
                        
                    return DataEdge(
                        edge_id=row['edge_id'],
                        source_id=row['source_id'],
                        target_id=row['target_id'],
                        operation=row['operation'],
                        timestamp=row['timestamp'],
                        metadata=metadata
                    )
                return None
        except Exception as e:
            logger.error(f"Error getting edge {edge_id}: {e}")
            return None
    
    def get_outgoing_edges(self, node_id: str) -> List[DataEdge]:
        """Get all outgoing edges from a node.
        
        Args:
            node_id: ID of the node
            
        Returns:
            List of DataEdge objects representing outgoing edges
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM edges WHERE source_id = ?", (node_id,))
                
                edges = []
                for row in cursor.fetchall():
                    try:
                        metadata = json.loads(row['metadata'])
                    except json.JSONDecodeError:
                        metadata = {}
                        
                    edge = DataEdge(
                        edge_id=row['edge_id'],
                        source_id=row['source_id'],
                        target_id=row['target_id'],
                        operation=row['operation'],
                        timestamp=row['timestamp'],
                        metadata=metadata
                    )
                    edges.append(edge)
                
                return edges
        except Exception as e:
            logger.error(f"Error getting outgoing edges for node {node_id}: {e}")
            return []
    
    def get_incoming_edges(self, node_id: str) -> List[DataEdge]:
        """Get all incoming edges to a node.
        
        Args:
            node_id: ID of the node
            
        Returns:
            List of DataEdge objects representing incoming edges
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM edges WHERE target_id = ?", (node_id,))
                
                edges = []
                for row in cursor.fetchall():
                    try:
                        metadata = json.loads(row['metadata'])
                    except json.JSONDecodeError:
                        metadata = {}
                        
                    edge = DataEdge(
                        edge_id=row['edge_id'],
                        source_id=row['source_id'],
                        target_id=row['target_id'],
                        operation=row['operation'],
                        timestamp=row['timestamp'],
                        metadata=metadata
                    )
                    edges.append(edge)
                
                return edges
        except Exception as e:
            logger.error(f"Error getting incoming edges for node {node_id}: {e}")
            return []
    
    def visualize(self, output_file: str = "data_lineage.html"):
        """Generate a visualization of the lineage graph.
        
        Args:
            output_file: Path to the output HTML file
        """
        try:
            try:
                import networkx as nx
                from pyvis.network import Network
            except ImportError:
                logger.error("Visualization requires networkx and pyvis. Install with: pip install networkx pyvis")
                return
            
            G = nx.DiGraph()
            
            # Add nodes to the graph
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Add nodes
                cursor.execute("SELECT * FROM nodes")
                for row in cursor.fetchall():
                    node_type = row['node_type']
                    color_map = {
                        'source': '#4CAF50',  # Green
                        'transformation': '#2196F3',  # Blue
                        'destination': '#F44336',  # Red
                        'dataset': '#FFC107'  # Yellow
                    }
                    color = color_map.get(node_type, '#9C27B0')  # Default purple
                    
                    G.add_node(
                        row['node_id'],
                        title=f"{row['name']}: {row['description']}",
                        label=row['name'],
                        color=color,
                        shape='dot',
                        size=25
                    )
                
                # Add edges
                cursor.execute("SELECT * FROM edges")
                for row in cursor.fetchall():
                    G.add_edge(
                        row['source_id'],
                        row['target_id'],
                        title=row['operation'],
                        label=row['operation']
                    )
            
            # Generate the visualization
            net = Network(height="750px", width="100%", directed=True, notebook=False)
            net.from_nx(G)
            net.save_graph(output_file)
            
            logger.info(f"Lineage visualization saved to {output_file}")
        except Exception as e:
            logger.error(f"Error generating visualization: {e}")
    
    def export_json(self, output_file: str = "data_lineage.json"):
        """Export the lineage graph to a JSON file.
        
        Args:
            output_file: Path to the output JSON file
        """
        try:
            lineage_data = {
                "nodes": [],
                "edges": []
            }
            
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Export nodes
                cursor.execute("SELECT * FROM nodes")
                for row in cursor.fetchall():
                    try:
                        metadata = json.loads(row['metadata'])
                    except json.JSONDecodeError:
                        metadata = {}
                        
                    node = {
                        "node_id": row['node_id'],
                        "node_type": row['node_type'],
                        "name": row['name'],
                        "description": row['description'],
                        "created_at": row['created_at'],
                        "metadata": metadata
                    }
                    lineage_data["nodes"].append(node)
                
                # Export edges
                cursor.execute("SELECT * FROM edges")
                for row in cursor.fetchall():
                    try:
                        metadata = json.loads(row['metadata'])
                    except json.JSONDecodeError:
                        metadata = {}
                        
                    edge = {
                        "edge_id": row['edge_id'],
                        "source_id": row['source_id'],
                        "target_id": row['target_id'],
                        "operation": row['operation'],
                        "timestamp": row['timestamp'],
                        "metadata": metadata
                    }
                    lineage_data["edges"].append(edge)
            
            with open(output_file, 'w') as f:
                json.dump(lineage_data, f, indent=2)
            
            logger.info(f"Lineage data exported to {output_file}")
        except Exception as e:
            logger.error(f"Error exporting lineage data: {e}")

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
                        node.metadata.update({"error": error_metadata})
                
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