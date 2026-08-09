import numpy as np
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class ColorRule:
    """
    Defines how a variable should be colored.
    """
    cmap: str = "viridis"
    vmin: Optional[float] = None
    vmax: Optional[float] = None
    symmetric: bool = False
    percentile_clip: Optional[float] = None


class ColorScaleManager:
    """
    Resolves vmin/vmax/cmap for geophysical variables.
    Matplotlib-only (HPC safe).
    """

    def __init__(self):
        self.rules = self._init_default_rules()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(
        self,
        values,
        var_name: Optional[str] = None
    ) -> Tuple[float, float, str]:
        """
        Determine vmin, vmax, and colormap for a variable.

        Parameters
        ----------
        values : array-like
            Data values (ObsValue)
        var_name : str
            CamelCase variable name (e.g. seaSurfaceTemperature)

        Returns
        -------
        vmin, vmax, cmap : float, float, str
        """
        values = np.asarray(values)
        values = values[np.isfinite(values)]

        if values.size == 0:
            return 0.0, 1.0, "viridis"

        rule = self.rules.get(var_name)

        cmap = rule.cmap if rule else "viridis"
        vmin = vmax = None

        # ---- Rule-driven scaling ----
        if rule:
            # Percentile clipping (robust to outliers)
            if rule.percentile_clip:
                p = rule.percentile_clip
                lo, hi = np.percentile(values, [100 - p, p])
                vmin, vmax = lo, hi

            # Symmetric scaling (anomalies)
            if rule.symmetric:
                m = np.nanmax(np.abs(values))
                vmin, vmax = -m, m

            # Hard physical limits override
            if rule.vmin is not None:
                vmin = rule.vmin
            if rule.vmax is not None:
                vmax = rule.vmax

        # ---- Robust fallback (never raw min/max) ----
        if vmin is None or vmax is None:
            lo, hi = np.percentile(values, [2, 98])
            vmin = lo if vmin is None else vmin
            vmax = hi if vmax is None else vmax

        # ---- Safety: zero span ----
        if np.isclose(vmin, vmax):
            delta = abs(vmin) * 0.1 if vmin != 0 else 1.0
            vmin -= delta
            vmax += delta

        return float(vmin), float(vmax), cmap

    # ------------------------------------------------------------------
    # Default rules (Matplotlib-native colormaps)
    # ------------------------------------------------------------------

    def _init_default_rules(self):
        return {

            # ---- Temperature ----
            "seaSurfaceTemperature": ColorRule(
                cmap="turbo", vmin=-2, vmax=35
            ),
            "airTemperature": ColorRule(
                cmap="coolwarm", vmin=-80, vmax=60
            ),

            # ---- Salinity ----
            "seaSurfaceSalinity": ColorRule(
                cmap="viridis", vmin=30, vmax=40
            ),
            "salinity": ColorRule(
                cmap="viridis", vmin=30, vmax=40
            ),

            # ---- Sea Ice ----
            "iceConcentration": ColorRule(
                cmap="Blues", vmin=0, vmax=1
            ),

            # ---- Altimetry / ADT ----
            "absoluteDynamicTopography": ColorRule(
                cmap="viridis", percentile_clip=99
            ),

            # ---- Anomalies ----
            "seaSurfaceTemperatureAnomaly": ColorRule(
                cmap="RdBu_r", symmetric=True, percentile_clip=98
            ),

            # ---- Aerosols ----
            "aerosolOpticalDepth": ColorRule(
                cmap="YlOrBr", vmin=0, vmax=2
            ),
        }
