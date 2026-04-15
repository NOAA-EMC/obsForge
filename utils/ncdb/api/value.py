import os
from ncdb.plotting.plot_generator import PlotGenerator

BASE_DATA_PRODUCTS_DIR = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/data_products/viewer"


class Value:
    def __init__(self, data, coords=None, metadata=None):
        self.data = data
        self.coords = coords or {}
        self.metadata = metadata or {}

    # -----------------------------
    # Core helpers
    # -----------------------------
    def is_scalar(self):
        try:
            return self.data.shape == ()
        except AttributeError:
            return True

    def _as_scalar(self):
        if not self.is_scalar():
            raise TypeError("Operation requires scalar Value")
        return self.data

    # -----------------------------
    # Representation
    # -----------------------------
    def __str__(self):
        if self.is_scalar():
            return str(self.data)
        return f"Value(shape={getattr(self.data, 'shape', None)})"

    def __repr__(self):
        if self.is_scalar():
            return f"<Value {self.data}>"
        return f"<Value shape={getattr(self.data, 'shape', None)}>"

    # -----------------------------
    # Safe scalar extraction
    # -----------------------------
    def item(self):
        return self._as_scalar()

    # -----------------------------
    # Conversions (scalar only)
    # -----------------------------
    def __float__(self):
        return float(self._as_scalar())

    def __int__(self):
        return int(self._as_scalar())

    def __bool__(self):
        raise TypeError("Truth value of Value is ambiguous")

    # -----------------------------
    # Arithmetic (scalar only)
    # -----------------------------
    def __add__(self, other):
        return self._as_scalar() + other

    def __radd__(self, other):
        return other + self._as_scalar()

    def __sub__(self, other):
        return self._as_scalar() - other

    def __rsub__(self, other):
        return other - self._as_scalar()

    def __mul__(self, other):
        return self._as_scalar() * other

    def __rmul__(self, other):
        return other * self._as_scalar()

    def __truediv__(self, other):
        return self._as_scalar() / other

    def __rtruediv__(self, other):
        return other / self._as_scalar()

    # -----------------------------
    # Comparisons (scalar only)
    # -----------------------------
    def __lt__(self, other):
        return self._as_scalar() < other

    def __le__(self, other):
        return self._as_scalar() <= other

    def __gt__(self, other):
        return self._as_scalar() > other

    def __ge__(self, other):
        return self._as_scalar() >= other

    def __eq__(self, other):
        return self._as_scalar() == other

    def __ne__(self, other):
        return self._as_scalar() != other

    '''
    def __array__(self, dtype=None):
        import numpy as np
        return np.asarray(self.data, dtype=dtype)

    TODO: Make all Value objects behave like arrays
    '''



    def to_plot_payload(self):
        if "latitude" not in self.coords or "longitude" not in self.coords:
            raise ValueError("Value has no latitude/longitude coordinates")

        return {
            "variable_name": self.metadata.get("variable_name"),
            "dataset_name": self.metadata.get("dataset_name"),
            "obs_space_name": self.metadata.get("obs_space_name"),
            "values": self.data,
            "lons": self.coords["longitude"],
            "lats": self.coords["latitude"],
            "units": self.metadata.get("units"),
        }

    '''
    def plot(self, plotter, out_path, interactive=False):
        payload = self.to_plot_payload()

        if interactive:
            plotter.generate_interactive_surface_map(out_path, payload)
        else:
            plotter.generate_surface_map(out_path, payload)
    '''

    def plot(self, filename=None, interactive=False):
        # needs to be cleaned up:
        if filename is None:
            dataset = self.metadata.get("dataset_name", "unknown")
            obs_space = self.metadata.get("obs_space_name", "unknown")

            output_dir = os.path.join(BASE_DATA_PRODUCTS_DIR, dataset)
            os.makedirs(output_dir, exist_ok=True)

            filename = f"{obs_space}.png" if not interactive else f"{obs_space}.html"
        else:
            output_dir = "."

        plotter = PlotGenerator(output_dir)
        out_path = os.path.join(output_dir, filename)

        payload = self.to_plot_payload()

        if interactive:
            plotter.generate_interactive_surface_map(out_path, payload)
        else:
            plotter.generate_surface_map(out_path, payload)

        return out_path
