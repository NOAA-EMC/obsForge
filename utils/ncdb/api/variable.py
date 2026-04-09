import logging
logger = logging.getLogger(__name__)

from datetime import datetime


class Variable:
    def __init__(self, field, variable_path, repo=None):
        self._field = field
        self._variable_path = variable_path
        self._repo = repo  # optional for lazy loading

    def __repr__(self):
        return f"<Variable {self._variable_path}>"

    def at(self, time):
        # 1. Find file for this time
        ds_file = self._field.find_file_for_time(time)

        if ds_file is None:
            raise ValueError(f"No data for time {time}")

        # 2. Ensure file is loaded (optional, depending on your design)
        if ds_file.netcdf_file is None:
            raise RuntimeError("DatasetFile has no NetCDF data loaded")

        # 3. Read variable
        values = ds_file.get_variable(self._variable_path)

        return values

    def __getitem__(self, time):
        return self.at(time)
