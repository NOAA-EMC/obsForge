import os
import logging
from datetime import datetime

# --- FAIL-SOFT IMPORTS ---
# We wrap imports to allow the pipeline to run even if plotting libs are missing.
try:
    import matplotlib
    matplotlib.use('Agg') # Force Headless mode (no GUI)
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
    
    Capabilities:
    - Graceful degradation (returns None if no Matplotlib).
    - Dual Plot generation (Full History + 7 Day Zoom).
    - Band Plots (Mean +/- StdDev).
    - Smart Date Axis formatting.
    """
    
    def __init__(self, output_dir):
        self.output_dir = output_dir
        if not HAS_MATPLOTLIB:
            logger.warning(f"Plotting disabled: {MISSING_LIB_MSG}")

    def generate_dual_plots(self, title, data, val_key, std_key, fname_base, y_label):
        """
        Generates two plots: Full History and Last 7 Days.
        Returns tuple of filenames (f_full, f_7d) or (None, None).
        """
        # 1. Dependency Check
        if not HAS_MATPLOTLIB:
            return None, None

        if not data:
            return None, None
        
        # 2. Data Preparation
        dates = []
        values = []
        stds = []
        
        for r in data:
            try:
                # Expects date='YYYYMMDD', cycle=HH (int)
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

        if not dates:
            return None, None

        # 3. Generate Full Plot
        f_full = f"{fname_base}_all.png"
        full_path = os.path.join(self.output_dir, f_full)
        success = self._plot_series(dates, values, stds, title, y_label, full_path)
        if not success: return None, None

        # 4. Generate Zoom Plot (Last 28 cycles ~= 7 days)
        f_7d = f"{fname_base}_7d.png"
        zoom_path = os.path.join(self.output_dir, f_7d)
        
        cutoff = -28 if len(dates) > 28 else 0
        if cutoff != 0:
            self._plot_series(dates[cutoff:], values[cutoff:], stds[cutoff:], title, y_label, zoom_path)
        else:
            # Not enough data for a distinct zoom, copy full plot
            import shutil
            shutil.copy(full_path, zoom_path)

        return f_full, f_7d

    def generate_dual_volume_plots(self, title, data, val_key, fname_base, y_label):
        """
        Wrapper for Volume plots.
        Uses the same Band Plot logic (since we now have Mean/Std for volumes).
        """
        return self.generate_dual_plots(title, data, val_key, "file_std", fname_base, y_label)

    def _plot_series(self, dates, values, stds, title, y_label, out_path):
        """Internal plotting logic."""
        try:
            fig, ax = plt.subplots(figsize=(10, 4))
            
            # Convert to numpy for vector operations
            d_arr = np.array(dates)
            v_arr = np.array(values)
            s_arr = np.array(stds)

            # --- PLOT LINE ---
            label = 'Mean' if np.any(s_arr > 0) else 'Value'
            ax.plot(d_arr, v_arr, color='#2980b9', linewidth=2, label=label, marker='.', markersize=4)
            
            # --- PLOT BAND ---
            if np.any(s_arr > 0):
                lower = v_arr - s_arr
                upper = v_arr + s_arr
                # Clamp lower bound to 0 for physical variables
                lower[lower < 0] = 0
                ax.fill_between(d_arr, lower, upper, color='#3498db', alpha=0.2, label='±1 \u03C3') # Sigma symbol

            # --- FORMATTING ---
            ax.set_title(title, fontsize=10, fontweight='bold', color='#333')
            ax.set_ylabel(y_label, fontsize=9)
            ax.grid(True, which='major', linestyle='--', alpha=0.6)
            ax.grid(True, which='minor', linestyle=':', alpha=0.3)
            
            # --- DATE AXIS HANDLING (Restored) ---
            # Smart locator: ticks every day, or every few hours depending on zoom
            if len(d_arr) > 14: # More than ~3 days of data (4 cycles/day)
                ax.xaxis.set_major_locator(mdates.DayLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
                ax.xaxis.set_minor_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
            else:
                ax.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                
            fig.autofmt_xdate()
            
            # Legend
            ax.legend(loc='upper left', fontsize='small', frameon=True)

            # Save
            plt.tight_layout()
            plt.savefig(out_path, dpi=100)
            plt.close(fig)
            return True
            
        except Exception as e:
            logger.error(f"Render failed for {out_path}: {e}")
            return False
