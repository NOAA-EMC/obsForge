import os
import logging
from datetime import datetime

# --- FAIL-SOFT IMPORTS ---
try:
    import matplotlib
    matplotlib.use('Agg') # Force Headless mode
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError as e:
    HAS_MATPLOTLIB = False
    MISSING_LIB_MSG = str(e)

logger = logging.getLogger("PlotGenerator")

class PlotGenerator:
    """
    Generates static PNG plots for the dashboard.
    Enforces Line Plots with Mean +/- StdDev bands.
    Guarantees full visibility of bands (no truncation).
    """
    
    def __init__(self, output_dir):
        self.output_dir = output_dir
        if not HAS_MATPLOTLIB:
            logger.warning(f"Plotting disabled: {MISSING_LIB_MSG}")

    def generate_dual_plots(self, title, data, val_key, std_key, fname_base, y_label, clamp_bottom=True):
        """
        Generates Full History and 7-Day Zoom plots.
        """
        if not HAS_MATPLOTLIB or not data: return None, None
        
        dates = []
        values = []
        stds = []
        
        for r in data:
            try:
                dt_str = f"{r['date']}{r['cycle']:02d}"
                dt = datetime.strptime(dt_str, "%Y%m%d%H")
                
                v = r.get(val_key)
                if v is None: continue
                
                s = r.get(std_key, 0.0) if std_key else 0.0
                
                dates.append(dt)
                values.append(v)
                stds.append(s)
            except Exception:
                continue

        if not dates: return None, None

        # Generate Full Plot
        f_full = f"{fname_base}_all.png"
        self._plot_series(dates, values, stds, title, y_label, os.path.join(self.output_dir, f_full), clamp_bottom)

        # Generate Zoom Plot (Last 28 cycles ~= 7 days)
        f_7d = f"{fname_base}_7d.png"
        zoom_path = os.path.join(self.output_dir, f_7d)
        cutoff = -28 if len(dates) > 28 else 0
        
        if cutoff != 0:
            self._plot_series(dates[cutoff:], values[cutoff:], stds[cutoff:], title, y_label, zoom_path, clamp_bottom)
        else:
            import shutil
            shutil.copy(os.path.join(self.output_dir, f_full), zoom_path)

        return f_full, f_7d

    def _plot_series(self, dates, values, stds, title, y_label, out_path, clamp_bottom):
        """Internal logic to draw Line + Band with safe limits."""
        try:
            fig, ax = plt.subplots(figsize=(10, 4))
            
            d_arr = np.array(dates)
            v_arr = np.array(values)
            s_arr = np.array(stds)

            # 1. Calculate Bands
            lower = v_arr - s_arr
            upper = v_arr + s_arr
            
            if clamp_bottom:
                lower[lower < 0] = 0
            
            # 2. Plot Band (First, so it's behind the line)
            # Only draw if there is actual variance
            has_band = np.any(s_arr > 0)
            if has_band:
                ax.fill_between(d_arr, lower, upper, color='#3498db', alpha=0.3, label='±1 \u03C3')

            # 3. Plot Mean Line
            label = 'Mean' if has_band else 'Value'
            ax.plot(d_arr, v_arr, color='#2980b9', linewidth=2, label=label, marker='.', markersize=4)

            # 4. Enforce Limits (Prevent truncation)
            # Calculate the absolute min/max including the band
            if has_band:
                y_min_data = np.min(lower)
                y_max_data = np.max(upper)
            else:
                y_min_data = np.min(v_arr)
                y_max_data = np.max(v_arr)
            
            # Add 5% padding
            span = y_max_data - y_min_data
            if span == 0: span = 1.0 # Handle flat line case
            
            y_limit_min = y_min_data - (span * 0.05)
            y_limit_max = y_max_data + (span * 0.05)
            
            # Respect clamp for the axis view too
            if clamp_bottom and y_limit_min < 0: 
                y_limit_min = 0
                
            ax.set_ylim(y_limit_min, y_limit_max)

            # 5. Formatting
            ax.set_title(title, fontsize=10, fontweight='bold', color='#333')
            ax.set_ylabel(y_label, fontsize=9)
            ax.grid(True, which='major', linestyle='--', alpha=0.6)
            ax.grid(True, which='minor', linestyle=':', alpha=0.3)
            
            # Date Axis
            if len(d_arr) > 14:
                ax.xaxis.set_major_locator(mdates.DayLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                
            fig.autofmt_xdate()
            ax.legend(loc='upper left', fontsize='small', frameon=True)

            plt.tight_layout()
            plt.savefig(out_path, dpi=100)
            plt.close(fig)
            return True
            
        except Exception as e:
            logger.error(f"Render failed for {out_path}: {e}")
            return False
