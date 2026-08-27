import json
import os
import numpy as np

# from processing.ioda_reader.reader import IodaReader
from .reader import IodaReader


class IodaSummaryProduct:
    """
    Generates a summary of a single IODA file for an obs_space.
    Can save/load the summary to/from JSON.
    """
    def __init__(self, run_type, cycle_id, obs_category, obs_space, file_path):
        self.run_type = run_type
        self.cycle_id = cycle_id
        self.obs_category = obs_category
        self.obs_space = obs_space
        self.file_path = file_path
        self.summary = None

    def generate(self):
        """Generate summary using IodaReader."""
        reader = IodaReader(self.file_path)

        surface = reader.get_surface_data()

        if surface is not None:
            values = surface["values"]
            # ensure it's a NumPy array
            values = np.asarray(values)
            nobs = values.size

            if nobs == 0:
                # no observations, safe defaults
                summary_surface = {
                    "var_name": surface["var_name"],
                    "units": surface["units"],
                    "count": 0,
                    "min": None,
                    "max": None,
                    "mean": None,
                    "std": None,
                }
            else:
                summary_surface = {
                    "var_name": surface["var_name"],
                    "units": surface["units"],
                    "count": int(nobs),
                    "min": float(np.nanmin(values)),
                    "max": float(np.nanmax(values)),
                    "mean": float(np.nanmean(values)),
                    "std": float(np.nanstd(values)),
                }
        else:
            summary_surface = None

        self.summary = {
            "surface": summary_surface,
            "obsvalue_dim": reader.get_obsvalue_dim(),
            "effective_dim": reader.get_effective_dim(),
        }

        return self.summary

    def old_generate(self):
        """Generate summary using IodaReader."""
        reader = IodaReader(self.file_path)
        # self.summary = {
            # "surface_data": reader.get_surface_data(),
            # "obsvalue_dim": reader.get_obsvalue_dim(),
            # "effective_dim": reader.get_effective_dim()
        # }

        surface = reader.get_surface_data()

        if surface is not None:
            values = surface["values"]
            summary_surface = {
                "var_name": surface["var_name"],
                "units": surface["units"],
                "count": int(values.count() if hasattr(values, "count") else values.size),
                "min": float(np.nanmin(values)),
                "max": float(np.nanmax(values)),
                "mean": float(np.nanmean(values)),
                "std": float(np.nanstd(values)),
            }
        else:
            summary_surface = None

        self.summary = {
            "surface": summary_surface,
            "obsvalue_dim": reader.get_obsvalue_dim(),
            "effective_dim": reader.get_effective_dim(),
        }

        return self.summary

    # def save_to_file(self, out_dir):
    def save_to_file(self, json_file_path):
        """
        Save summary to JSON in the directory:
          <out_dir>/<run_type>/<cycle_id>/<obs_category>/<obs_space>.json
        """
        if self.summary is None:
            raise RuntimeError("Summary not generated yet")

        # json_dir = os.path.join(out_dir, self.run_type, self.cycle_id, self.obs_category)
        # os.makedirs(json_dir, exist_ok=True)
        # out_path = os.path.join(json_dir, f"{self.obs_space}.json")

        # with open(out_path, "w") as f:
        with open(json_file_path, "w") as f:
            json.dump(self.summary, f, indent=2)
        return json_file_path

    @staticmethod
    def load_from_file(json_file):
        """Load summary from a JSON file."""
        with open(json_file, "r") as f:
            return json.load(f)

