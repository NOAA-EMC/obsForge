import logging
import numpy as np

logger = logging.getLogger(__name__)


class DerivedAttributeRegistry:
    """Registry of derived numeric attributes for variables."""

    def __init__(self):
        self._available_metrics = {}

    def register(self, name, func):
        self._available_metrics[name] = func

    def compute_for_array(self, array, metrics=None):
        """
        Compute a subset of registered metrics for a given array.
        If metrics is None, computes all.
        """
        target = metrics if metrics else self._available_metrics.keys()
        results = {}
        
        # Ensure we only process numeric data and handle masks
        if not hasattr(array, "size") or array.size == 0:
            return results

        for name in target:
            if name in self._available_metrics:
                try:
                    # Logic to handle masked arrays vs standard numpy
                    val = self._available_metrics[name](array)
                    results[name] = float(val) if not np.isnan(val) else None
                except Exception as e:
                    logger.debug(f"Metric {name} failed: {e}")
        return results

    @classmethod
    def default(cls):
        registry = cls()
        registry.register("min", lambda x: np.ma.min(x) if hasattr(x, 'mask') else np.min(x))
        registry.register("max", lambda x: np.ma.max(x) if hasattr(x, 'mask') else np.max(x))
        registry.register("mean", lambda x: np.ma.mean(x) if hasattr(x, 'mask') else np.mean(x))
        registry.register("std_dev", lambda x: np.ma.std(x) if hasattr(x, 'mask') else np.std(x))
        registry.register("nobs", lambda x: x.count() if hasattr(x, 'mask') else x.size)
        # registry.register("median", lambda x: float(np.median(x)))
        return registry



'''
import logging
import numpy as np

logger = logging.getLogger(__name__)


class DerivedAttributeRegistry:
    """Registry of derived numeric attributes for variables."""

    def __init__(self):
        self._attributes = {}

    def register(self, name, func):
        self._attributes[name] = func

    def compute_attributes(self, array):
        return {name: func(array) for name, func in self._attributes.items()}

    @classmethod
    def default(cls):
        registry = cls()
        registry.register("min", lambda x: float(np.min(x)))
        registry.register("max", lambda x: float(np.max(x)))
        registry.register("mean", lambda x: float(np.mean(x)))
        registry.register("std_dev", lambda x: float(np.std(x)))
        registry.register("median", lambda x: float(np.median(x)))
        registry.register("nobs", lambda x: int(x.size))
        return registry

class DerivedAttribute:
    def __init__(self, name: str, value: float):
        self.name = name
        self.value = value
'''

