import os
import re
import fnmatch
from typing import List, Tuple, Optional
from .file import File


class FileScanner:
    def scan(self, root_path: str) -> List[File]:
        """Default scan method (leaf directories)."""
        return self.get_all_leaf_files(root_path)

    # @staticmethod
    # def get_all_leaf_files(root_path: str) -> List[File]:
    def get_all_leaf_files(self, root_path: str) -> List[File]:
        """
        Traverses the directory tree and creates File objects 
        ONLY for files found in leaf directories.
        """
        # print(f"get_all_leaf_files: {root_path}")
        root_path = os.path.realpath(root_path)
        all_files: List[File] = []

        for dirpath, dirnames, filenames in os.walk(root_path):
            # A directory is a leaf if it contains no further subdirectories
            if not dirnames:
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    # Leveraging from_path to handle os.stat and realpath
                    all_files.append(File.from_path(full_path))
        
        # print(f"get_all_leaf_files: {all_files}")
        return all_files

    @staticmethod
    def filter_files(files: List[File], pattern: str) -> Tuple[List[File], List[File]]:
        """
        Partitions the list of File objects into two lists based on the filename.
        Returns: (selected_files, rejected_files)
        """
        selected_files: List[File] = []
        rejected_files: List[File] = []

        for f in files:
            # filter on the basename (e.g., 'gfs.t06z.rads_adt_6a.nc')
            filename = os.path.basename(f.path)
            
            if fnmatch.fnmatch(filename, pattern):
                selected_files.append(f)
            else:
                rejected_files.append(f)

        return selected_files, rejected_files


class SubdirFileScanner(FileScanner):
    def __init__(self, subdir: str):
        self.subdir = subdir

    def scan(self, root_path: str) -> List[File]:
        scan_dir = os.path.join(root_path, self.subdir)
        return super().scan(scan_dir)



class ObsSpaceNameParser:
    """
    Base class for filename parsers that extract ObsSpace names
    and generate file search patterns.
    """

    def parse(self, path: str, prefix: Optional[str] = None) -> Optional[str]:
        raise NotImplementedError

    def get_search_pattern(self, prefix: str, hour: str) -> str:
        raise NotImplementedError


class ObsForgeObsSpaceNameParser(ObsSpaceNameParser):

    SEPARATOR = "."
    EXTENSION = "nc"
    NAME_INDEX = 2
    EXPECTED_PARTS = 4

    def parse(self, path: str, prefix: Optional[str] = None) -> Optional[str]:
        filename = os.path.basename(path)
        parts = filename.split(self.SEPARATOR)
        if len(parts) != self.EXPECTED_PARTS or parts[-1] != self.EXTENSION:
            return None
        if prefix is not None and parts[0] != prefix:
            return None
        if not re.fullmatch(r"t\d{2}z", parts[1]):
            return None
        return parts[self.NAME_INDEX]

    def get_search_pattern(self, prefix: str, hour: str) -> str:
        if isinstance(hour, int):
            hour = f"{hour:02d}"
        return f"{prefix}.t{hour}z.*.nc"


class NcObsSpaceNameParser(ObsSpaceNameParser):

    EXTENSION = ".nc"

    def parse(self, path: str, prefix: Optional[str] = None) -> Optional[str]:
        filename = os.path.basename(path)
        if not filename.endswith(self.EXTENSION):
            return None

        name = filename[:-len(self.EXTENSION)]
        if prefix is not None and name != prefix:
            return None

        return name

    def get_search_pattern(self, prefix: str, hour: str) -> str:
        """
        Hour is ignored for this naming scheme.
        """
        return "*.nc"
