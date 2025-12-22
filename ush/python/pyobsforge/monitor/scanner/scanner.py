from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

@dataclass
class FileInventoryData:
    """Represents a single physical file found on disk."""
    rel_path: str
    category: str
    obs_space_name: str
    integrity: str      # OK, CORRUPT, MISSING, EMPTY
    size_bytes: int
    obs_count: int = 0
    error_msg: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict) # Learned metadata

@dataclass
class TaskRunData:
    """Represents a single execution of a task."""
    task_name: str
    run_type: str
    logfile: str
    
    # Execution Details
    job_id: Optional[str] = None
    status: Optional[str] = None
    exit_code: Optional[int] = None
    attempt: Optional[int] = None
    host: Optional[str] = None
    
    # Timing
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    runtime_sec: float = 0.0
    
    # Inventory
    files: List[FileInventoryData] = field(default_factory=list)

@dataclass
class CycleData:
    date: str
    cycle: int
    tasks: List[TaskRunData] = field(default_factory=list)
