import logging
logger = logging.getLogger(__name__)

import os
from datetime import datetime
from .value import Value

# A field is a time-dependent scalar field.
# it could be a single scalar (derived attribute)
# or it could be a field over a 2d geographic domain
# the coordinates for the 2d variable are taken
# from the associated field (lon, lat)

class Field:
    def __init__(self, field, variable_path, repo=None, derived_name=None):
        self._field = field
        self._variable_path = variable_path
        self._repo = repo
        self._derived_name = derived_name

    def __repr__(self):
        if self._derived_name:
            return f"<Field {self._variable_path}.{self._derived_name}>"
        return f"<Field {self._variable_path}>"

    def at(self, time):
        ds_file = self._field.find_file_for_time(time)

        if ds_file is None:
            raise ValueError(f"No data for time {time}")

        if ds_file.netcdf_file is None:
            raise RuntimeError("DatasetFile has no NetCDF data loaded")

        # --- base variable ---
        if self._derived_name is None:
            # return ds_file.get_variable(self._variable_path)

            data = ds_file.get_variable(self._variable_path)

            # minimal coordinate resolution (temporary)
            lat = ds_file.get_variable("/MetaData/latitude")
            lon = ds_file.get_variable("/MetaData/longitude")

            coords = {}
            if lat is not None and lon is not None:
                coords = {
                    "latitude": lat,
                    "longitude": lon,
                }

            return Value(data=data, coords=coords)

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

        # return value
        return Value(
            data=value,
            coords={},   # scalar → no spatial coords
            metadata={"derived": self._derived_name}
        )

    def __getitem__(self, time):
        return self.at(time)

    def __getattr__(self, name: str):
        # prevent messing with internals
        if name.startswith("_"):
            raise AttributeError(name)

        if self._field.has_derived(self._variable_path, name):
            return Field(
                field=self._field,
                variable_path=self._variable_path,
                repo=self._repo,
                derived_name=name
            )

        raise AttributeError(f"{name} not found")

    def plot(self, out_file, t1=None, t2=None, n_cycles=None):
        if self._derived_name is None:
            raise ValueError("Field.plot() only supported for derived fields")

        if self._repo is None:
            raise RuntimeError("Field.plot() requires repository access")

        session = self._repo.session

        from ncdb.ds.services.field_data_service import FieldDataService
        from ncdb.plotting.plot_generator import PlotGenerator

        service = FieldDataService(session)

        # --- fetch full dataframe ---
        df = service.get_variable_derived_data(
            self._field,
            self._variable_path
        )

        if df.empty:
            raise ValueError("No data available for plotting")

        # --- filter ---
        if n_cycles is not None:
            df = df.tail(n_cycles)

        elif t1 is not None or t2 is not None:
            if t1 is not None:
                df = df[df["time"] >= t1]
            if t2 is not None:
                df = df[df["time"] <= t2]

        # --- plotting ---
        plotter = PlotGenerator(os.path.dirname(out_file) or ".")

        plotter.generate_history_plot_pd(
            df=df,
            val_col=self._derived_name,   # e.g. "mean", "max"
            std_col="stddev",            # optional
            title=f"{self._variable_path}.{self._derived_name}",
            y_label="Value",
            out_path=out_file
        )

        return out_file
