import numpy as np
import logging

logger = logging.getLogger(__name__)


def subsample_surface_points(
    lats,
    lons,
    values,
    max_points=300_000,
    seed=42,
):
    """
    Subsample surface observation points to a maximum count.

    Parameters
    ----------
    lats, lons, values : np.ndarray
        1D arrays of equal length
    max_points : int
        Maximum number of points to keep
    seed : int or None
        Random seed for reproducibility (None = nondeterministic)

    Returns
    -------
    lats_s, lons_s, values_s : np.ndarray
        Possibly subsampled arrays
    was_subsampled : bool
    """

    n = len(values)

    if n <= max_points:
        return lats, lons, values, False

    if seed is not None:
        rng = np.random.default_rng(seed)
        idx = rng.choice(n, max_points, replace=False)
    else:
        idx = np.random.choice(n, max_points, replace=False)

    logger.info(
        f"Subsampling surface obs: {n:,} → {max_points:,}"
    )

    return lats[idx], lons[idx], values[idx], True
