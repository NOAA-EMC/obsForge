import logging
logger = logging.getLogger(__name__)

from datetime import datetime


class Variable:
    def __init__(self, field, variable_path, repo=None, derived_name=None):
        self._field = field
        self._variable_path = variable_path
        self._repo = repo
        self._derived_name = derived_name

    def __repr__(self):
        if self._derived_name:
            return f"<Variable {self._variable_path}.{self._derived_name}>"
        return f"<Variable {self._variable_path}>"

    def at(self, time):
        ds_file = self._field.find_file_for_time(time)

        if ds_file is None:
            raise ValueError(f"No data for time {time}")

        if ds_file.netcdf_file is None:
            raise RuntimeError("DatasetFile has no NetCDF data loaded")

        # --- base variable ---
        if self._derived_name is None:
            return ds_file.get_variable(self._variable_path)

        # --- derived variable ---
        # value = ds_file.get_derived(self._variable_path, self._derived_name)

        # temporarily ugly:
        nc_file = ds_file.netcdf_file
        nc_file.from_orm_derived_attributes(self._repo.session)
        values = nc_file.derived_values.get(self._variable_path)
        value = values.get(self._derived_name)

        if value is None:
            raise ValueError(
                f"Derived attribute '{self._derived_name}' not found for {self._variable_path}"
            )

        return value

    def __getitem__(self, time):
        return self.at(time)

    def __getattr__(self, name: str):
        # prevent messing with internals
        if name.startswith("_"):
            raise AttributeError(name)

        if self._field.has_derived(self._variable_path, name):
            return Variable(
                field=self._field,
                variable_path=self._variable_path,
                repo=self._repo,
                derived_name=name
            )
            # return DerivedVariable(self._field, self._variable_path, name)

        raise AttributeError(f"{name} not found")
