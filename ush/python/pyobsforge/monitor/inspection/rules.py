import logging
import json
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("InspectionRules")

class InspectionRule:
    def check(self, file_record, context) -> str:
        """Returns error message or None."""
        raise NotImplementedError

class IodaStructureRule(InspectionRule):
    """Checks for required IODA groups."""
    def check(self, f, ctx) -> str:
        props = f.get('properties')
        if not props: return None

        try:
            if isinstance(props, str): props = json.loads(props)
            
            # Allow empty files to pass structure check (handled by ZeroObsRule)
            if f['obs_count'] == 0: pass

            schema = props.get('schema', {})
            # Check for ANY ObsValue or MetaData group keys
            has_obs = any(k.startswith("ObsValue") for k in schema.keys())
            has_meta = any(k.startswith("MetaData") for k in schema.keys())

            if not has_obs: return "Invalid IODA: Missing 'ObsValue' group"
            if not has_meta: return "Invalid IODA: Missing 'MetaData' group"

        except Exception as e:
            return f"Structure Check Error: {e}"
            
        return None

class ZeroObsRule(InspectionRule):
    def check(self, f, ctx) -> str:
        if f['obs_count'] == 0: return "Zero Observations"
        return None

class VolumeAnomalyRule(InspectionRule):
    def check(self, f, ctx) -> str:
        key = (f['obs_space'], f['run_type'])
        baseline = ctx.get('baselines', {}).get(key, 0)
        
        if baseline > 0 and f['obs_count'] < baseline:
            return f"Low Volume ({f['obs_count']} < {baseline})"
        return None

class GeoSpatialRule(InspectionRule):
    def check(self, f, ctx) -> str:
        errors = []
        
        # Latitude
        if f['min_lat'] is not None and f['max_lat'] is not None:
            if f['min_lat'] < -90.0 or f['max_lat'] > 90.0:
                errors.append(f"Lat OutOfBounds [{f['min_lat']:.1f}, {f['max_lat']:.1f}]")
            if f['min_lat'] > f['max_lat']:
                errors.append(f"Lat Inverted ({f['min_lat']} > {f['max_lat']})")

        # Longitude
        if f['min_lon'] is not None and f['max_lon'] is not None:
            if f['min_lon'] < -180.0 or f['max_lon'] > 360.0:
                errors.append(f"Lon OutOfBounds [{f['min_lon']:.1f}, {f['max_lon']:.1f}]")
        
        return "; ".join(errors) if errors else None

class DataQualityRule(InspectionRule):
    """Checks for data quality flags reported by the Scanner."""
    def check(self, f, ctx) -> str:
        props = f.get('properties')
        if not props: return None
        
        try:
            if isinstance(props, str): props = json.loads(props)
            
            outliers = props.get('outliers', [])
            if outliers:
                return f"Bad Data: {', '.join(outliers)}"
        except: pass
        return None

class TimeConsistencyRule(InspectionRule):
    """Checks if data timestamps match the filename cycle."""
    def check(self, f, ctx) -> str:
        if f.get('start_time') is None: return None
        
        try:
            # Calculate Cycle Epoch
            cycle_dt = datetime.strptime(f"{f['date']}{f['cycle']:02d}", "%Y%m%d%H")
            cycle_epoch = cycle_dt.replace(tzinfo=timezone.utc).timestamp()
            
            # Allow +/- 9 hour window
            window = 9 * 3600
            
            if abs(f['start_time'] - cycle_epoch) > window:
                file_dt = datetime.fromtimestamp(f['start_time'], timezone.utc).strftime('%Y-%m-%d %H:%M')
                return f"Time Mismatch: Data starts {file_dt}, Cycle is {cycle_dt.strftime('%H:%M')}"
                
        except Exception:
            pass # Ignore date parsing errors
            
        return None
