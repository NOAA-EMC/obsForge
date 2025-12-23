from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

@dataclass
class FileInventoryData:
    """Represents a single physical file found on disk."""
    rel_path: str
    category: str
    obs_space_name: str
    
    integrity: str         # OK, CORRUPT, MISSING
    size_bytes: int
    mtime: int             # Last Modified Time (Unix Epoch)
    obs_count: int
    error_msg: Optional[str] = None
    
    # Raw metadata (Attributes, Schema)
    properties: Dict[str, Any] = field(default_factory=dict)
    
    # Deep Metrics (Calculated only if file changed)
    stats: List[Dict] = field(default_factory=list)  # Min/Max/Std per variable
    domain: Optional[Dict] = None                    # Lat/Lon/Time bounds

@dataclass
class TaskRunData:
    """Represents the execution of a specific task."""
    task_name: str
    run_type: str
    logfile: str
    job_id: Optional[str] = None
    status: Optional[str] = None
    exit_code: Optional[int] = None
    attempt: Optional[int] = None
    host: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    runtime_sec: Optional[float] = None
    
    files: List[FileInventoryData] = field(default_factory=list)

@dataclass
class CycleData:
    date: str
    cycle: int
    tasks: List[TaskRunData] = field(default_factory=list)
