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
        Handles masked arrays to prevent NaN-related database IntegrityErrors.
        """
        results = {}
        
        if not hasattr(array, "size") or array.size == 0:
            return results

        target = metrics if metrics else self._available_metrics.keys()

        # nobs = number of unmasked (valid) observations
        nobs = int(array.count()) if hasattr(array, 'mask') else int(array.size)
        
        for name in target:
            if name not in self._available_metrics:
                continue
                
            try:
                if name == "nobs":
                    results["nobs"] = nobs
                elif name == "nmissing":
                    results["nmissing"] = int(array.size - nobs)
                elif nobs > 0:
                    val = self._available_metrics[name](array)
                    
                    # Convert to standard Python float/int if not NaN
                    if not np.isnan(val):
                        results[name] = float(val)
                    else:
                        # Explicitly skip adding to dict to avoid NULL in NOT NULL columns
                        logger.debug(f"Metric {name} resulted in NaN for node, skipping.")
                
                else:
                    # nobs == 0: We skip min/max/mean entirely so no row is created 
                    # or no NULL value is sent to the DB.
                    logger.debug(f"Skipping math metric {name} because array is 100% masked.")
                    
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
        registry.register("nmissing", lambda x: x.size - x.count() if hasattr(x, 'mask') else 0)
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

