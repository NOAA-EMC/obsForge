import logging
import json
import hashlib
from typing import List, Dict, Optional, Any
from sqlalchemy.orm import Session
# from .netcdf_structure_orm import (
    # NetcdfStructureORM,
    # NetcdfNodeORM,
    # NetcdfStructureAttributeORM,
    # NetcdfVariableDimensionORM,
# )
# from .netcdf_node import NetcdfNode, read_netcdf_nodes
from .netcdf_structure_orm import NetcdfStructureORM, NetcdfVariableDimensionORM
from .netcdf_scanner import NetcdfScanner
from .netcdf_node import NetcdfNode

# import netCDF4

logger = logging.getLogger(__name__)


class NetcdfStructure:
    """
    Domain object representing the invariant skeleton of a NetCDF file.
    """

    def __init__(
        self, 
        nodes_by_path: Dict[str, "NetcdfNode"], 
        root: "NetcdfNode",
        id: Optional[int] = None
    ):
        self.id = id
        self.nodes_by_path = nodes_by_path
        self.root = root
        
        # Populate pointers: Ensure all nodes know they belong to this structure
        for node in self.nodes_by_path.values():
            node.structure = self
            
        self.structure_hash = self._generate_hash()

    @classmethod
    def from_file(cls, file_path: str) -> "NetcdfStructure":
        # Use the scanner in skeleton mode (no values)
        root, nodes_by_path = NetcdfScanner.scan(file_path, read_values=False)
        s = cls(nodes_by_path=nodes_by_path, root=root)
        # logger.info(
            # f"From file: {s}"
            # "\n"
            # "---------------------------\n"
            # f"{s.to_json()}"
            # "---------------------------\n"
        # )
        return s


    def _generate_hash(self) -> str:
        """
        Creates a deterministic SHA256 hash based on the structural 
        identity of all nodes in the registry.
        """
        # Sort by path to ensure the hash is consistent regardless of scan order
        sorted_keys = sorted(self.nodes_by_path.keys())
        identities = [self.nodes_by_path[k].get_structural_identity() for k in sorted_keys]
        
        struct_json = json.dumps(identities, sort_keys=True)
        return hashlib.sha256(struct_json.encode()).hexdigest()

    @classmethod
    def from_orm(cls, orm: NetcdfStructureORM) -> "NetcdfStructure":
        """Reconstructs the full tree and registry."""
        if not orm: return None

        nodes_by_path = {}
        # 1. Map flat ORM nodes to domain nodes
        for n_orm in orm.nodes:
            d_node = NetcdfNode.from_orm_self(n_orm)
            nodes_by_path[d_node.full_path] = d_node

        # 2. Build Hierarchy & Resolve Dimensions
        root = nodes_by_path.get("/")
        for n_orm in orm.nodes:
            d_node = nodes_by_path[n_orm.full_path]
            
            # Parenting
            if d_node.full_path != "/":
                parent_path = d_node.full_path.rsplit("/", 1)[0] or "/"
                parent = nodes_by_path.get(parent_path)
                if parent: parent.add_child(d_node)

            # Ordered Dimensions
            if n_orm.node_type == "VARIABLE":
                d_node.dim_names = [
                    assoc.dimension.full_path for assoc in n_orm.variable_dimensions
                ]

        return cls(nodes_by_path=nodes_by_path, root=root, id=orm.id)

    def to_db(self, session: Session) -> Optional[NetcdfStructureORM]:
        """
        Persists the structure, then recursively persists its nodes 
        and dimension relationships.
        """
        try:
            # 1. Sync/Insert the Structure
            struct_orm = (
                session.query(NetcdfStructureORM)
                .filter_by(structure_hash=self.structure_hash)
                .first()
            )

            if not struct_orm:
                struct_orm = NetcdfStructureORM(structure_hash=self.structure_hash)
                session.add(struct_orm)
                session.flush()

            self.id = struct_orm.id

            # 2. Persist all Nodes
            # We sort by path length to process the root and groups before deep variables
            for path in sorted(self.nodes_by_path.keys(), key=len):
                try:
                    self.nodes_by_path[path].to_db(session)
                except Exception as e:
                    logger.error(f"Failed to persist node '{path}': {e}")
                    continue

            # 3. Persist Dimension Links (Association Table)
            self._persist_dimension_links(session)

            return struct_orm

        except Exception as e:
            logger.error(f"Critical error persisting structure {self.structure_hash}: {e}")
            return None

    def _persist_dimension_links(self, session: Session):
        """
        Resolves dimension names to node IDs and populates the 
        NetcdfVariableDimensionORM association table.
        """
        for node in self.nodes_by_path.values():
            if node.node_type == "VARIABLE" and node.dim_names:
                for idx, d_name in enumerate(node.dim_names):
                    try:
                        # Find the actual node for this dimension
                        dim_node = self._resolve_dim_node(node, d_name)
                        if not dim_node or not dim_node.id:
                            logger.warning(f"Could not resolve dimension '{d_name}' for variable '{node.full_path}'")
                            continue

                        # Check for existing relationship
                        exists = (
                            session.query(NetcdfVariableDimensionORM)
                            .filter_by(
                                variable_node_id=node.id,
                                dimension_node_id=dim_node.id,
                                dim_index=idx
                            )
                            .first()
                        )

                        if not exists:
                            session.add(NetcdfVariableDimensionORM(
                                variable_node_id=node.id,
                                dimension_node_id=dim_node.id,
                                dim_index=idx
                            ))
                    except Exception as e:
                        logger.error(f"Error linking dim '{d_name}' to var '{node.full_path}': {e}")
                        continue

    def _resolve_dim_node(self, var_node: "NetcdfNode", dim_name: str) -> Optional["NetcdfNode"]:
        """
        Finds the dimension node by name. In NetCDF, a variable looks for 
        dimensions in its own group first, then moves up to parent groups.
        """
        # logger.info(f"DEBUG [Resolver]: Searching for dim '{dim_name}' for var '{var_node.full_path}'")
        # 1. Check if dim_name is actually a full path already
        if dim_name.startswith("/") and dim_name in self.nodes_by_path:
            return self.nodes_by_path[dim_name]

        # 2. Search up the hierarchy (NetCDF-4 scoping rules)
        current_search_path = var_node.full_path.rsplit("/", 1)[0]
        while True:
            candidate = f"{current_search_path.rstrip('/')}/{dim_name}"
        
            # Check if the candidate exists in our registry
            exists = candidate in self.nodes_by_path
            node_type = self.nodes_by_path[candidate].node_type if exists else "N/A"
            
            # logger.info(f"[Resolver]:   Checking candidate '{candidate}' -> Found: {exists} (Type: {node_type})")
            
            if exists:
                return self.nodes_by_path[candidate]
            
            if current_search_path == "" or current_search_path == "/":
                break
            current_search_path = current_search_path.rsplit("/", 1)[0]
        
        # logger.info(f"[Resolver]:   FAILED to find '{dim_name}'. Available paths in registry: {list(self.nodes_by_path.keys())[:10]}...")
        return None


    '''
            # Construct candidate path: /path/to/group/dim_name
            candidate = f"{current_search_path.rstrip('/')}/{dim_name}"
            if candidate in self.nodes_by_path:
                node = self.nodes_by_path[candidate]
                if node.node_type == "DIMENSION":
                    return node
            if current_search_path == "" or current_search_path == "/":
                break
            current_search_path = current_search_path.rsplit("/", 1)[0]
        return None
    '''

    def to_json(self, indent: int = 2) -> str:
        """
        Returns a JSON string representation of the entire structure.
        """
        if not self.root:
            return "{}"
            
        # We include the hash at the top level for easy reference
        output = {
            "structure_hash": self.structure_hash,
            "root": self.root.to_dict()
        }
        
        return json.dumps(output, indent=indent)

    def __repr__(self) -> str:
        return (
            f"<NetcdfStructure id = {self.id}, "
            f"{self.structure_hash}"
        )

    def find_node(self, path: str) -> Optional["NetcdfNode"]:
        """
        O(1) lookup to find a node by its full path.
        Returns None if the path is not part of this structure.
        """
        node = self.nodes_by_path.get(path)
        if not node:
            logger.debug(f"Node not found in structure for path: {path}")
        return node

    def list_variables(self, parent_path: str = "/") -> List[str]:
        """
        Returns a list of full paths for all VARIABLE nodes under the given parent path.
        If parent_path is '/', it returns all variables in the file.
        """
        parent_path = parent_path.rstrip("/")
        if not parent_path:
            parent_path = "/"

        variables = []
        for path, node in self.nodes_by_path.items():
            if node.node_type == "VARIABLE":
                # Check if the variable is inside the requested group
                if parent_path == "/" or path.startswith(f"{parent_path}/"):
                    variables.append(path)
        
        return sorted(variables)
