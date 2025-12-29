import os
import logging
from datetime import datetime

# --- FAIL-SOFT IMPORTS ---
# Allows the pipeline to run even if Matplotlib is missing (e.g. on a minimal server).
try:
    import matplotlib
    matplotlib.use('Agg') # Force Headless mode (no GUI window)
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
    
    Features:
    - Dual Plots: Generates both 'Full History' and '7-Day Zoom' images.
    - Smart Bands: 
        * If std_key is provided -> Plots 'Spatial Variance' (Mean +/- StdDev from DB).
        * If std_key is None -> Plots 'Temporal Variance' (Horizontal Band of Historical Mean +/- StdDev).
    - Safe Limits: Ensures bands are never cut off by the chart edges.
    """
    
    def __init__(self, output_dir):
        self.output_dir = output_dir
        if not HAS_MATPLOTLIB:
            logger.warning(f"Plotting disabled: {MISSING_LIB_MSG}")

    def generate_dual_plots(self, title, data, val_key, std_key, fname_base, y_label, clamp_bottom=True):
        """
        Main entry point. Orchestrates data prep and calling the renderer twice.
        
        Args:
            title (str): Chart title.
            data (list): List of dicts from DataService.
            val_key (str): Key for the main line value (e.g., 'total_obs').
            std_key (str): Key for the band width (e.g., 'std_dev'). Pass None for Temporal Mode.
            fname_base (str): Output filename prefix.
            y_label (str): Y-axis label.
            clamp_bottom (bool): If True, bands stop at 0 (for Counts). False for Physics (e.g. Temp).
        """
        # 1. Dependency & Data Check
        if not HAS_MATPLOTLIB or not data:
            return None, None
        
        dates = []
        values = []
        stds = []
        
        # 2. Parse Data
        for r in data:
            try:
                # Construct datetime object (YYYYMMDD + HH)
                dt_str = f"{r['date']}{r['cycle']:02d}"
                dt = datetime.strptime(dt_str, "%Y%m%d%H")
                
                v = r.get(val_key)
                if v is None: continue
                
                # If std_key is provided, extract it. Else None (triggers Temporal Mode).
                s = r.get(std_key) if std_key else None
                
                dates.append(dt)
                values.append(v)
                stds.append(s)
            except Exception:
                continue

        if not dates:
            return None, None

        # 3. Generate Full History Plot
        f_full = f"{fname_base}_all.png"
        self._plot_series(dates, values, stds, title, y_label, os.path.join(self.output_dir, f_full), clamp_bottom)

        # 4. Generate 7-Day Zoom Plot
        f_7d = f"{fname_base}_7d.png"
        zoom_path = os.path.join(self.output_dir, f_7d)
        
        # Slice last 28 points (approx 7 days @ 4 cycles/day)
        cutoff = -28 if len(dates) > 28 else 0
        
        if cutoff != 0:
            self._plot_series(dates[cutoff:], values[cutoff:], stds[cutoff:], title, y_label, zoom_path, clamp_bottom)
        else:
            # If dataset is small, zoom plot is identical to full plot
            import shutil
            shutil.copy(os.path.join(self.output_dir, f_full), zoom_path)

        return f_full, f_7d

    def _plot_series(self, dates, values, stds, title, y_label, out_path, clamp_bottom):
        """Internal method to render the Matplotlib figure."""
        try:
            fig, ax = plt.subplots(figsize=(10, 4))
            
            d_arr = np.array(dates)
            v_arr = np.array(values)
            
            # MODE CHECK: Do we have per-point Standard Deviation from the DB?
            has_spatial_std = (stds[0] is not None)
            
            if has_spatial_std:
                # --- MODE 1: PHYSICAL VARIANCE (SPATIAL) ---
                # The band represents variation *inside* the file (e.g., Temp variance).
                s_arr = np.array(stds)
                
                # Plot Mean Line
                ax.plot(d_arr, v_arr, color='#2980b9', linewidth=2, label='Mean', marker='.', markersize=4)
                
                # Plot Variable Band
                lower = v_arr - s_arr
                upper = v_arr + s_arr
                if clamp_bottom: lower[lower < 0] = 0
                ax.fill_between(d_arr, lower, upper, color='#3498db', alpha=0.3, label='±1 \u03C3 (Spatial)')
                
                # Calculate Limits for visibility
                y_min = np.min(lower)
                y_max = np.max(upper)
                
            else:
                # --- MODE 2: TEMPORAL VARIANCE (HISTORICAL) ---
                # The band represents stability *over time*.
                # Calculated from the time series itself.
                global_mean = np.mean(v_arr)
                global_std = np.std(v_arr)
                
                # Plot Actual Value Line
                ax.plot(d_arr, v_arr, color='#2980b9', linewidth=2, label='Value', marker='.', markersize=4)
                
                # Calculate Horizontal Band
                lower_bound = global_mean - global_std
                upper_bound = global_mean + global_std
                if clamp_bottom and lower_bound < 0: lower_bound = 0
                
                # Draw Horizontal Reference Line & Band
                ax.axhline(y=global_mean, color='#e67e22', linestyle='--', alpha=0.8, linewidth=1, label=f'Avg ({global_mean:.1f})')
                ax.axhspan(lower_bound, upper_bound, color='#e67e22', alpha=0.15, label='±1 \u03C3 (Temporal)')
                
                # Calculate Limits to ensure band is visible even if line is flat
                y_min = min(np.min(v_arr), lower_bound)
                y_max = max(np.max(v_arr), upper_bound)

            # --- COMMON FORMATTING ---
            ax.set_title(title, fontsize=10, fontweight='bold', color='#333')
            ax.set_ylabel(y_label, fontsize=9)
            ax.grid(True, which='major', linestyle='--', alpha=0.6)
            ax.grid(True, which='minor', linestyle=':', alpha=0.3)
            
            # Set Y-Limits with 10% padding so band doesn't touch the frame
            span = y_max - y_min
            if span == 0: span = 1.0 if y_max == 0 else y_max * 0.1
            
            limit_min = y_min - (span * 0.1)
            if clamp_bottom and limit_min < 0: limit_min = 0
            
            ax.set_ylim(limit_min, y_max + (span * 0.1))

            # Date Axis Formatting
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
