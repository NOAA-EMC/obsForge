import logging
import json
from datetime import datetime, timezone

logger = logging.getLogger("InspectionRules")

class InspectionRule:
    """Base class for all inspection logic."""
    def check(self, file_record, context) -> str:
        """Returns error string if failed, None if passed."""
        raise NotImplementedError

# ------------------------------------------------------------------------------
# 1. STRUCTURAL CHECKS
# ------------------------------------------------------------------------------

class IodaStructureRule(InspectionRule):
    """Checks if the file has the mandatory IODA groups (ObsValue, MetaData)."""
    def check(self, f, ctx) -> str:
        props = f.get('properties')
        if not props: return None

        try:
            if isinstance(props, str): props = json.loads(props)
            
            # Skip empty files (handled by ZeroObsRule)
            if f['obs_count'] == 0: return None

            schema = props.get('schema', {})
            has_obs = any(k.startswith("ObsValue") for k in schema.keys())
            has_meta = any(k.startswith("MetaData") for k in schema.keys())

            if not has_obs: return "Invalid IODA: Missing 'ObsValue' group"
            if not has_meta: return "Invalid IODA: Missing 'MetaData' group"

        except Exception as e:
            return f"Structure Check Error: {e}"
            
        return None

class ZeroObsRule(InspectionRule):
    """Flags files that exist but contain 0 observations."""
    def check(self, f, ctx) -> str:
        if f['obs_count'] == 0:
            return "Zero Observations"
        return None

# ------------------------------------------------------------------------------
# 2. DATA CONTENT CHECKS (Physics & Bounds)
# ------------------------------------------------------------------------------

class PhysicalRangeRule(InspectionRule):
    """
    Checks if physical variables are within valid earth ranges defined in DB.
    - Flags outliers/fill values as ERRORS.
    - Detects Unit Mismatch (Celsius vs Kelvin).
    - Checks for Frozen Sensors (Zero Variance).
    """

    def check(self, f, ctx) -> str:
        try:
            # Stats come from InspectionDataService.get_file_stats
            # Each stat dict includes: min, max, mean, valid_min, valid_max, units
            stats = ctx['stats_loader'](f['id'])
        except Exception:
            return None

        errors = []
        celsius_detected = False

        for s in stats:
            var_name = s['name'].split('/')[-1]
            
            # --- A. CHECK FROZEN SENSOR ---
            # If min_std_dev is set (usually 0.0) and actual std_dev is <= that
            if s.get('min_std_dev') is not None and s['std_dev'] <= s['min_std_dev']:
                # Only flag if we have enough samples to be sure
                if f['obs_count'] > 10:
                    errors.append(f"{var_name} Frozen (StdDev {s['std_dev']})")

            # --- B. CHECK PHYSICAL BOUNDS ---
            # Skip if no limits defined in DB
            if s.get('valid_min') is None or s.get('valid_max') is None:
                continue

            v_min, v_max, v_mean = s['min'], s['max'], s['mean']
            limit_min, limit_max = s['valid_min'], s['valid_max']
            
            # 1. Check against DB Limits (Primary Unit, usually Kelvin)
            is_valid = (v_min >= limit_min and v_max <= limit_max)
            
            if is_valid:
                continue # Data is perfect

            # 2. If invalid, try Celsius Hypothesis (for Temperature only)
            # Heuristic: If DB expects Kelvin (>250) but Mean is low (<100)
            if 'kelvin' in (s.get('units') or '').lower() and v_mean < 100:
                # Convert File Stats (C -> K)
                conv_min = v_min + 273.15
                conv_max = v_max + 273.15
                
                # Re-Check
                if conv_min >= limit_min and conv_max <= limit_max:
                    celsius_detected = True
                    continue # It is valid Celsius!
            
            # 3. If still invalid, it's a Gross Error
            if v_min < limit_min:
                errors.append(f"{var_name} Underflow ({v_min:.1f} < {limit_min})")
            if v_max > limit_max:
                errors.append(f"{var_name} Overflow ({v_max:.1f} > {limit_max})")

        # Result Logic
        if errors:
            return "; ".join(errors) # Warning/Failure
        
        if celsius_detected:
            return "INFO: Celsius Units Detected" # Informational
            
        return None

class DataQualityRule(InspectionRule):
    """Checks for specific flags raised by the Scanner during file parsing."""
    def check(self, f, ctx) -> str:
        props = f.get('properties')
        if not props: return None
        
        try:
            if isinstance(props, str): props = json.loads(props)
            outliers = props.get('outliers', [])
            if outliers:
                return f"Scanner Flags: {', '.join(outliers)}"
        except: pass
        return None

# ------------------------------------------------------------------------------
# 3. HISTORICAL & TEMPORAL CHECKS
# ------------------------------------------------------------------------------

class VolumeAnomalyRule(InspectionRule):
    """Checks if volume is significantly below the 30-day baseline."""
    def check(self, f, ctx) -> str:
        key = (f['obs_space'], f['run_type'])
        baseline = ctx.get('baselines', {}).get(key, 0)
        
        if baseline > 0 and f['obs_count'] < baseline:
            return f"Low Volume ({f['obs_count']} < {baseline})"
        return None

class TimeConsistencyRule(InspectionRule):
    """Checks if data timestamps match the filename cycle."""
    def check(self, f, ctx) -> str:
        if f.get('start_time') is None: return None
        
        try:
            cycle_dt = datetime.strptime(f"{f['date']}{f['cycle']:02d}", "%Y%m%d%H")
            cycle_epoch = cycle_dt.replace(tzinfo=timezone.utc).timestamp()
            window = 9 * 3600 # +/- 9 hours
            
            if abs(f['start_time'] - cycle_epoch) > window:
                file_dt = datetime.fromtimestamp(f['start_time'], timezone.utc).strftime('%Y-%m-%d %H:%M')
                return f"Time Mismatch: Data starts {file_dt}, Cycle is {cycle_dt.strftime('%H:%M')}"
        except Exception:
            pass 
        return None
