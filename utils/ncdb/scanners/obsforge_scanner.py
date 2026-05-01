from .base import BaseScanner

class ObsForgeScanner(BaseScanner):

def parse_obs_space(self, file_path):
    filename = os.path.basename(file_path)
    parts = filename.split(".")

    if len(parts) != 4 or parts[-1] != "nc":
        return None

    if not re.fullmatch(r"t\d{2}z", parts[1]):
        return None

    return parts[2]

def select_files(self, files, dataset_name, cycle_hour):
    pattern = f"{dataset_name}.t{cycle_hour}z.*.nc"

    import fnmatch
    return [
        f for f in files
        if fnmatch.fnmatch(os.path.basename(f.path), pattern)
    ]


