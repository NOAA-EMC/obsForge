import json
import logging
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
        if not props:
            return None

        try:
            if isinstance(props, str):
                props = json.loads(props)

            # Skip empty files (handled by ZeroObsRule)
            if f['obs_count'] == 0:
                return None

            schema = props.get('schema', {})
            has_obs = any(k.startswith("ObsValue") for k in schema.keys())
            has_meta = any(k.startswith("MetaData") for k in schema.keys())

            if not has_obs:
                return "Invalid IODA: Missing 'ObsValue' group"
            if not has_meta:
                return "Invalid IODA: Missing 'MetaData' group"

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
    Checks physical variables.

    1. FROZEN CHECK: Only applies to variables that have Physical Bounds defined.
       (Skips metadata like 'ocean_basin' which are expected to be constant).
    2. BOUNDS CHECK: Flags Overflow/Underflow.
    3. UNIT INFERENCE: Detects Kelvin vs Celsius.
    """

    def check(self, f, ctx) -> str:
        try:
            stats = ctx['stats_loader'](f['id'])
        except Exception:
            return None

        errors = []
        celsius_detected = False

        for s in stats:
            var_name = s['name'].split('/')[-1]

            # Helper: Does this variable have physical limits defined in the DB?
            has_physics = (
                s.get('valid_min') is not None and
                s.get('valid_max') is not None
            )

            # --- A. CHECK FROZEN SENSOR ---
            # Logic: Only check for "Frozen" if it's a Physical Variable.
            if has_physics:
                threshold = s.get('min_std_dev', 0.0)
                if s['std_dev'] <= threshold:
                    # Ignore if too few observations to be statistically sound
                    if f['obs_count'] > 10:
                        errors.append(
                            f"{var_name} Frozen (StdDev {s['std_dev']})"
                        )

            # --- B. CHECK PHYSICAL BOUNDS ---
            if not has_physics:
                continue

            v_min, v_max, v_mean = s['min'], s['max'], s['mean']
            limit_min, limit_max = s['valid_min'], s['valid_max']

            # 1. Check against DB Limits
            is_valid = (v_min >= limit_min and v_max <= limit_max)

            if is_valid:
                continue

            # 2. Try Celsius Hypothesis
            # Heuristic: If DB expects Kelvin (>250) but Mean is low (<100)
            is_kelvin_db = 'kelvin' in (s.get('units') or '').lower()
            if is_kelvin_db and v_mean < 100:
                conv_min = v_min + 273.15
                conv_max = v_max + 273.15
                if conv_min >= limit_min and conv_max <= limit_max:
                    celsius_detected = True
                    continue

            # 3. Gross Error
            if v_min < limit_min:
                errors.append(
                    f"{var_name} Underflow ({v_min:.1f} < {limit_min})"
                )
            if v_max > limit_max:
                errors.append(
                    f"{var_name} Overflow ({v_max:.1f} > {limit_max})"
                )

        if errors:
            return "; ".join(errors)

        if celsius_detected:
            return "INFO: Celsius Units Detected"

        return None


class DataQualityRule(InspectionRule):
    """Checks for specific flags raised by the Scanner (Zero Tolerance for Fill Values)."""
    def check(self, f, context):
        props = f.get('properties')
        if not props:
            return None

        try:
            import json
            if isinstance(props, str):
                props = json.loads(props)
            
            outliers = props.get('outliers', [])
            if outliers:
                # Flag as a Critical Failure if ANY garbage is found
                return f"Garbage Data: {', '.join(outliers)}"
        except Exception:
            pass
        return None


# ------------------------------------------------------------------------------
# 3. HISTORICAL & TEMPORAL CHECKS
# ------------------------------------------------------------------------------

class VolumeAnomalyRule(InspectionRule):
    """Checks if volume is significantly below the 30-day baseline."""
    def check(self, f, ctx) -> str:
        key = (f['obs_space'], f['run_type'])
        baseline = ctx.get('baselines', {}).get(key, 0)

        # Only flag if we have a baseline and current count is strictly lower
        if baseline > 0 and f['obs_count'] < baseline:
            return f"Low Volume ({f['obs_count']} < {baseline})"
        return None


class TimeConsistencyRule(InspectionRule):
    """Checks if data timestamps match the filename cycle."""
    def check(self, f, ctx) -> str:
        # 1. Safety Check: If Scanner failed to get time, we can't check consistency
        if f.get('start_time') is None:
            return None

        try:
            # 2. Calculate Cycle Epoch (UTC)
            # Format: YYYYMMDDHH
            cycle_str = f"{f['date']}{f['cycle']:02d}"
            cycle_dt = datetime.strptime(cycle_str, "%Y%m%d%H")
            
            # Force UTC for the cycle to match standard Epoch
            cycle_epoch = cycle_dt.replace(tzinfo=timezone.utc).timestamp()

            # 3. Validation Window (Expanded to 12 hours to be safe against Timezone jitter)
            window = 12 * 3600

            # Check difference
            if abs(f['start_time'] - cycle_epoch) > window:
                # Formatting for readable error message
                file_dt = datetime.fromtimestamp(
                    f['start_time'], timezone.utc
                ).strftime('%Y-%m-%d %H:%M')
                
                return (
                    f"Time Mismatch: Data starts {file_dt}, "
                    f"Cycle is {cycle_dt.strftime('%H:%M')}"
                )

        except Exception:
            pass  # Ignore parsing errors if filename format is weird

        return None
