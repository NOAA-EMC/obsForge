import os
import fnmatch
from typing import List, Tuple
from .file import File


class FileScanner:
    @staticmethod
    def get_all_leaf_files(root_path: str) -> List[File]:
        """
        Traverses the directory tree and creates File objects 
        ONLY for files found in leaf directories.
        """
        root_path = os.path.realpath(root_path)
        all_files: List[File] = []

        for dirpath, dirnames, filenames in os.walk(root_path):
            # A directory is a leaf if it contains no further subdirectories
            if not dirnames:
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    # Leveraging your existing from_path to handle os.stat and realpath
                    all_files.append(File.from_path(full_path))
        
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
            # We filter based on the basename (e.g., 'gfs.t06z.rads_adt_6a.nc')
            filename = os.path.basename(f.path)
            
            if fnmatch.fnmatch(filename, pattern):
                selected_files.append(f)
            else:
                rejected_files.append(f)

        return selected_files, rejected_files
