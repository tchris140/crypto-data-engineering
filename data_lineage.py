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
        self._init_db()
    
    def _init_db(self):
        """Initialize the SQLite database schema if it doesn't exist."""
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
    
    def add_node(self, node_type: str, name: str, description: str, 
                 metadata: Optional[Dict[str, Any]] = None) -> str:
        """Add a new data node to the lineage graph.
        
        Args:
            node_type: Type of node ('source', 'transformation', 'destination')
            name: Name of the node
            description: Description of the node
            metadata: Additional metadata about the node
            
        Returns:
            node_id: ID of the created node
        """
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
            
        logger.info(f"Added node: {node.name} ({node.node_id})")
        return node_id
    
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
        edge_id = str(uuid.uuid4())
        timestamp = datetime.datetime.now().isoformat()
        metadata = metadata or {}
        
        edge = DataEdge(
            edge_id=edge_id,
            source_id=source_id,
            target_id=target_id,
            operation=operation,
            timestamp=timestamp,
            metadata=metadata
        )
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO edges VALUES (?, ?, ?, ?, ?, ?)",
                (edge.edge_id, edge.source_id, edge.target_id, edge.operation,
                 edge.timestamp, json.dumps(edge.metadata))
            )
            conn.commit()
            
        logger.info(f"Added edge: {operation} from {source_id} to {target_id}")
        return edge_id
    
    def get_node(self, node_id: str) -> Optional[DataNode]:
        """Get a node by its ID.
        
        Args:
            node_id: ID of the node to retrieve
            
        Returns:
            DataNode or None if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
            row = cursor.fetchone()
            
            if row:
                return DataNode(
                    node_id=row['node_id'],
                    node_type=row['node_type'],
                    name=row['name'],
                    description=row['description'],
                    created_at=row['created_at'],
                    metadata=json.loads(row['metadata'])
                )
            return None
    
    def get_edge(self, edge_id: str) -> Optional[DataEdge]:
        """Get an edge by its ID.
        
        Args:
            edge_id: ID of the edge to retrieve
            
        Returns:
            DataEdge or None if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM edges WHERE edge_id = ?", (edge_id,))
            row = cursor.fetchone()
            
            if row:
                return DataEdge(
                    edge_id=row['edge_id'],
                    source_id=row['source_id'],
                    target_id=row['target_id'],
                    operation=row['operation'],
                    timestamp=row['timestamp'],
                    metadata=json.loads(row['metadata'])
                )
            return None
    
    def get_upstream_nodes(self, node_id: str) -> List[DataNode]:
        """Get all nodes that are upstream of the given node.
        
        Args:
            node_id: ID of the node to get upstream nodes for
            
        Returns:
            List of upstream DataNodes
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT n.* FROM nodes n
                JOIN edges e ON n.node_id = e.source_id
                WHERE e.target_id = ?
            """, (node_id,))
            
            upstream_nodes = []
            for row in cursor.fetchall():
                upstream_nodes.append(DataNode(
                    node_id=row['node_id'],
                    node_type=row['node_type'],
                    name=row['name'],
                    description=row['description'],
                    created_at=row['created_at'],
                    metadata=json.loads(row['metadata'])
                ))
            
            return upstream_nodes
    
    def get_downstream_nodes(self, node_id: str) -> List[DataNode]:
        """Get all nodes that are downstream of the given node.
        
        Args:
            node_id: ID of the node to get downstream nodes for
            
        Returns:
            List of downstream DataNodes
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT n.* FROM nodes n
                JOIN edges e ON n.node_id = e.target_id
                WHERE e.source_id = ?
            """, (node_id,))
            
            downstream_nodes = []
            for row in cursor.fetchall():
                downstream_nodes.append(DataNode(
                    node_id=row['node_id'],
                    node_type=row['node_type'],
                    name=row['name'],
                    description=row['description'],
                    created_at=row['created_at'],
                    metadata=json.loads(row['metadata'])
                ))
            
            return downstream_nodes
    
    def visualize(self, output_file: str = "data_lineage.html"):
        """Generate a visualization of the lineage graph.
        
        Args:
            output_file: Path to the output HTML file
        """
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
    
    def export_json(self, output_file: str = "data_lineage.json"):
        """Export the lineage graph to a JSON file.
        
        Args:
            output_file: Path to the output JSON file
        """
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
                node = {
                    "node_id": row['node_id'],
                    "node_type": row['node_type'],
                    "name": row['name'],
                    "description": row['description'],
                    "created_at": row['created_at'],
                    "metadata": json.loads(row['metadata'])
                }
                lineage_data["nodes"].append(node)
            
            # Export edges
            cursor.execute("SELECT * FROM edges")
            for row in cursor.fetchall():
                edge = {
                    "edge_id": row['edge_id'],
                    "source_id": row['source_id'],
                    "target_id": row['target_id'],
                    "operation": row['operation'],
                    "timestamp": row['timestamp'],
                    "metadata": json.loads(row['metadata'])
                }
                lineage_data["edges"].append(edge)
        
        with open(output_file, 'w') as f:
            json.dump(lineage_data, f, indent=2)
        
        logger.info(f"Lineage data exported to {output_file}")

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
        self.source_nodes = source_nodes if isinstance(source_nodes, list) else [source_nodes]
        self.operation = operation
        self.target_name = target_name
        self.target_description = target_description
        self.target_type = target_type
        self.metadata = metadata or {}
        self.target_id = None
        self.lineage = get_lineage_tracker()
    
    def __enter__(self):
        """Create the target node when entering the context."""
        self.target_id = self.lineage.add_node(
            node_type=self.target_type,
            name=self.target_name,
            description=self.target_description,
            metadata=self.metadata
        )
        
        # Create edges from source nodes to target
        for source_id in self.source_nodes:
            self.lineage.add_edge(
                source_id=source_id,
                target_id=self.target_id,
                operation=self.operation,
                metadata=self.metadata
            )
        
        return self.target_id
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Handle any errors when exiting the context."""
        if exc_type is not None:
            # Log the error in metadata
            error_metadata = {
                "error_type": str(exc_type.__name__),
                "error_message": str(exc_val)
            }
            
            # Update the target node with error information
            with sqlite3.connect(self.lineage.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT metadata FROM nodes WHERE node_id = ?",
                    (self.target_id,)
                )
                result = cursor.fetchone()
                if result:
                    metadata = json.loads(result[0])
                    metadata.update({"error": error_metadata})
                    
                    cursor.execute(
                        "UPDATE nodes SET metadata = ? WHERE node_id = ?",
                        (json.dumps(metadata), self.target_id)
                    )
                    conn.commit()
            
            logger.error(f"Error in lineage context: {exc_val}")

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