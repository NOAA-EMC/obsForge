import os
import logging
import numpy as np
from datetime import datetime, timedelta

# --- OPTIONAL DEPENDENCY: MATPLOTLIB ---
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.ticker as ticker
    # Force non-interactive backend
    plt.switch_backend('Agg')
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

logger = logging.getLogger("PlotGenerator")

class PlotGenerator:
    """
    Generates static PNG plots.
    If Matplotlib is missing, it skips generation safely.
    """
    
    def __init__(self, output_dir):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Colors
        self.c_mean = '#1f77b4'
        self.c_band = '#a6cee3'
        self.c_vol  = '#2ca02c'
        self.c_vol_fill = '#98df8a'

        if not HAS_MATPLOTLIB:
            logger.warning("Matplotlib not found. Plot generation will be skipped.")

    def generate_dual_plots(self, title, data, value_key, std_key, base_filename, y_label):
        if not HAS_MATPLOTLIB: return None, None
        
        f_full = f"{base_filename}_all.png"
        self._create_band_plot(title + " (Full)", data, value_key, std_key, f_full, y_label, None)
        
        f_7d = f"{base_filename}_7d.png"
        self._create_band_plot(title + " (7 Days)", data, value_key, std_key, f_7d, y_label, 7)
        
        return f_full, f_7d

    def generate_dual_volume_plots(self, title, data, value_key, base_filename, y_label):
        if not HAS_MATPLOTLIB: return None, None
        
        f_full = f"{base_filename}_all.png"
        self._create_volume_plot(title + " (Full)", data, value_key, f_full, y_label, None)
        
        f_7d = f"{base_filename}_7d.png"
        self._create_volume_plot(title + " (7 Days)", data, value_key, f_7d, y_label, 7)
        
        return f_full, f_7d

    # --- INTERNAL ---

    def _prepare_data(self, data_list, days_filter=None):
        dates = []
        clean_data = []
        cutoff = datetime.now() - timedelta(days=days_filter) if days_filter else None

        for r in data_list:
            try:
                dt_str = f"{r['date']}{int(r['cycle']):02d}"
                dt = datetime.strptime(dt_str, "%Y%m%d%H")
                if cutoff and dt < cutoff: continue
                dates.append(dt)
                clean_data.append(r)
            except: continue
        return dates, clean_data

    def _create_band_plot(self, title, raw_data, val_key, std_key, filename, y_label, days_limit):
        dates, subset = self._prepare_data(raw_data, days_limit)
        if not dates: return

        values = np.array([r[val_key] for r in subset], dtype=float)
        upper = values
        lower = values
        
        if std_key:
            stds = np.array([r.get(std_key, 0.0) for r in subset], dtype=float)
            upper = values + stds
            lower = values - stds

        fig, ax = plt.subplots(figsize=(6, 3))
        ax.plot(dates, values, color=self.c_mean, linewidth=1.5, label='Mean')
        if std_key:
            ax.fill_between(dates, lower, upper, color=self.c_band, alpha=0.4, label='±1 \u03C3')
            ax.legend(loc='upper right', fontsize=8)

        self._format_axis(ax, title, y_label, dates)
        plt.savefig(os.path.join(self.output_dir, filename), dpi=80, bbox_inches='tight')
        plt.close(fig)

    def _create_volume_plot(self, title, raw_data, val_key, filename, y_label, days_limit):
        dates, subset = self._prepare_data(raw_data, days_limit)
        if not dates: return

        values = np.array([r[val_key] for r in subset], dtype=float)
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.plot(dates, values, color=self.c_vol, linewidth=1.5)
        ax.fill_between(dates, 0, values, color=self.c_vol_fill, alpha=0.3)
        
        self._format_axis(ax, title, y_label, dates)
        ax.yaxis.set_major_formatter(ticker.EngFormatter())
        plt.savefig(os.path.join(self.output_dir, filename), dpi=80, bbox_inches='tight')
        plt.close(fig)

    def _format_axis(self, ax, title, y_label, dates):
        ax.set_title(title, fontsize=10, fontweight='bold', pad=10)
        ax.set_ylabel(y_label, fontsize=8)
        ax.tick_params(axis='both', which='major', labelsize=8)
        ax.grid(True, linestyle=':', alpha=0.6)
        fig = ax.get_figure()
        fig.autofmt_xdate()
        span = (max(dates) - min(dates)).days
        fmt = '%H:%M' if span <= 2 else '%m-%d'
        ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
