import logging
import os
import shutil
from datetime import datetime
import pandas as pd
from typing import Optional, List, Union

# --- FAIL-SOFT IMPORTS ---
# Allows the pipeline to run even if Matplotlib is missing.
try:
    import matplotlib
    matplotlib.use('Agg')  # Force Headless mode (no GUI window)
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import numpy as np
    import cartopy
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    # from .var_plot_scales import VariablePlotScales
    from .subsample_surface import subsample_surface_points
    from .color_scales import ColorScaleManager

    HAS_MATPLOTLIB = True
    MISSING_LIB_MSG = ""
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
        * If std_key is provided -> Plots 'Spatial Variance'
          (Mean +/- StdDev from DB).
        * If std_key is None -> Plots 'Temporal Variance'
          (Horizontal Band of Historical Mean +/- StdDev).
    - Safe Limits: Ensures bands are never cut off by the chart edges.
    """

    def __init__(self, output_dir):
        self.output_dir = output_dir

        self.color_manager = ColorScaleManager()

        if not HAS_MATPLOTLIB:
            logger.warning(f"Plotting disabled: {MISSING_LIB_MSG}")
        else:
            # Get the directory where THIS file (plot_generator.py) lives
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cartopy_data_dir = os.path.join(base_dir, "cartopy_data")

            if os.path.exists(cartopy_data_dir):
                cartopy.config['data_dir'] = cartopy_data_dir
                # Force cartopy to ONLY look here and not try to download
                cartopy.config['pre_existing_data_dir'] = cartopy_data_dir
            else:
                print(f"Warning: Cartopy data not found at {cartopy_data_dir}")

    def generate_history_plot(
        self,
        title,
        data,
        val_key,
        std_key,
        fname,
        y_label,
        days=None,
        clamp_bottom=True,
    ):
        """
        Generate a single time-series plot.

        Args:
            days (int | None):
                - None → entire history
                - N → last N days (assuming 4 cycles/day)
        """
        if not HAS_MATPLOTLIB or not data:
            return None

        dates = []
        values = []
        stds = []

        # Parse data
        for r in data:
            try:
                dt_str = f"{r['date']}{r['cycle']:02d}"
                dt = datetime.strptime(dt_str, "%Y%m%d%H")

                v = r.get(val_key)
                if v is None:
                    continue

                s = r.get(std_key) if std_key else None

                dates.append(dt)
                values.append(v)
                stds.append(s)
            # except Exception:
                # continue
            except Exception as e:
                logger.error(f"Bad record skipped: {r} ({e})")


        if not dates:
            return None

        # --- WINDOWING LOGIC ---
        if days is not None:
            points = days * 4  # 4 cycles/day
            if len(dates) > points:
                dates = dates[-points:]
                values = values[-points:]
                stds = stds[-points:]

        out_path = os.path.join(self.output_dir, fname)

        self._plot_series(
            dates,
            values,
            stds,
            title,
            y_label,
            out_path,
            clamp_bottom,
        )

        return fname


    def generate_history_plot_with_moving_avg(
        self,
        plot_path,
        data,
        title,
        val_key,
        std_key,
        y_label,
        days=None,
        clamp_bottom=True,
    ):
        """
        Generate a single time-series plot.

        Args:
            days (int | None):
                - None → entire history
                - N → last N days (assuming 4 cycles/day)
        """
        if not HAS_MATPLOTLIB or not data:
            return None

        dates = []
        values = []
        stds = []

        # Parse data
        for r in data:
            try:
                dt_str = f"{r['date']}{r['cycle']:02d}"
                dt = datetime.strptime(dt_str, "%Y%m%d%H")

                v = r.get(val_key)
                if v is None:
                    continue

                s = r.get(std_key) if std_key else None

                dates.append(dt)
                values.append(v)
                stds.append(s)
            # except Exception:
                # continue
            except Exception as e:
                logger.error(f"Bad record skipped: {r} ({e})")


        if not dates:
            return None

        # --- WINDOWING LOGIC ---
        if days is not None:
            points = days * 4  # 4 cycles/day
            if len(dates) > points:
                dates = dates[-points:]
                values = values[-points:]
                stds = stds[-points:]

        # out_path = os.path.join(self.output_dir, fname)

        v_arr = np.array(values)
        mvalues, mstds = self._moving_avg(v_arr, N=120)

        self._plot_series_with_moving_avg(
            dates,
            values,
            mvalues,
            mstds,
            stds,
            title,
            y_label,
            plot_path,
            clamp_bottom,
        )

    def _plot_series(
        self, dates, values, stds, title, y_label, out_path, clamp_bottom
    ):
        """Internal method to render the Matplotlib figure."""
        try:
            fig, ax = plt.subplots(figsize=(10, 4))

            d_arr = np.array(dates)
            v_arr = np.array(values)

            # MODE CHECK: Do we have per-point Standard Deviation from the DB?
            has_spatial_std = (stds[0] is not None)
            # has_spatial_std = any(s is not None for s in stds)

            if has_spatial_std:
                # --- MODE 1: PHYSICAL VARIANCE (SPATIAL) ---
                # The band represents variation *inside* the file.
                s_arr = np.array(stds)

                # Plot Mean Line
                ax.plot(
                    d_arr, v_arr, color='#2980b9', linewidth=2,
                    label='Mean', marker='.', markersize=4
                )

                # Plot Variable Band
                lower = v_arr - s_arr
                upper = v_arr + s_arr
                if clamp_bottom:
                    lower[lower < 0] = 0

                ax.fill_between(
                    d_arr, lower, upper, color='#3498db', alpha=0.3,
                    label='±1 \u03C3 (Spatial)'
                )

                # Calculate Limits for visibility
                y_min = np.min(lower)
                y_max = np.max(upper)

            else:
                # --- MODE 2: TEMPORAL VARIANCE (HISTORICAL) ---
                # The band represents stability *over time*.
                global_mean = np.mean(v_arr)
                global_std = np.std(v_arr)

                # Plot Actual Value Line
                ax.plot(
                    d_arr, v_arr, color='#2980b9', linewidth=2,
                    label='Value', marker='.', markersize=4
                )

                # Calculate Horizontal Band
                lower_bound = global_mean - global_std
                upper_bound = global_mean + global_std
                if clamp_bottom and lower_bound < 0:
                    lower_bound = 0

                # Draw Horizontal Reference Line & Band
                ax.axhline(
                    y=global_mean, color='#e67e22', linestyle='--',
                    alpha=0.8, linewidth=1, label=f'Avg ({global_mean:.1f})'
                )
                ax.axhspan(
                    lower_bound, upper_bound, color='#e67e22', alpha=0.15,
                    label='±1 \u03C3 (Temporal)'
                )

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
            if span == 0:
                span = 1.0 if y_max == 0 else y_max * 0.1

            limit_min = y_min - (span * 0.1)
            if clamp_bottom and limit_min < 0:
                limit_min = 0

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


    def _plot_series_with_moving_avg(
        self,
        dates,
        values,
        mvalues=None,
        mstds=None,
        stds=None,
        title="",
        y_label="",
        out_path=None,
        clamp_bottom=None,
    ):
        try:
            fig, ax = plt.subplots(figsize=(10, 4))

            # Convert to numpy arrays for calculations
            v_arr = np.array(values)
            d_arr = np.array(dates)
            mvalues_arr = np.array(mvalues) if mvalues is not None else None
            mstds_arr = np.array(mstds) if mstds is not None else None

            # MODE CHECK: Do we have per-point Standard Deviation from the DB?
            has_spatial_std = (stds[0] is not None)
            # has_spatial_std = any(s is not None for s in stds)

            if has_spatial_std:
                # --- MODE 1: PHYSICAL VARIANCE (SPATIAL) ---
                # The band represents variation *inside* the file.
                s_arr = np.array(stds)

                # Plot Mean Line
                ax.plot(
                    d_arr, v_arr, color='#2980b9', linewidth=2,
                    label='Mean', marker='.', markersize=4
                )

                # Plot Variable Band
                lower = v_arr - s_arr
                upper = v_arr + s_arr
                if clamp_bottom:
                    lower[lower < 0] = 0

                ax.fill_between(
                    d_arr, lower, upper, color='#3498db', alpha=0.3,
                    label='±1 \u03C3 (Spatial)'
                )

                # Calculate Limits for visibility
                y_min = np.min(lower)
                y_max = np.max(upper)

            else:
                # --- MODE 2: TEMPORAL VARIANCE (HISTORICAL) ---
                # The band represents stability *over time*.


                # -------------------------------
                # 1. Plot the actual value line
                # -------------------------------
                ax.plot(
                    d_arr, v_arr,
                    color='#2980b9',        # Original blue
                    linewidth=2,
                    label='Value',
                    marker='.',             # small dot marker
                    markersize=4
                )

                # -------------------------------
                # 2. Plot the moving average line
                # -------------------------------
                if mvalues_arr is not None:
                    ax.plot(
                        d_arr, mvalues_arr,
                        color='#e67e22',    # Orange line for moving average
                        linewidth=2,
                        label='Moving Avg'
                    )

                # -------------------------------
                # 3. Draw the band around moving average using moving std deviation
                # -------------------------------
                if mvalues_arr is not None and mstds_arr is not None:
                    lower_bound = mvalues_arr - mstds_arr
                    upper_bound = mvalues_arr + mstds_arr

                    # Clamp bottom if requested
                    if clamp_bottom is not None:
                        lower_bound = np.maximum(lower_bound, clamp_bottom)

                    # Fill band around moving average
                    ax.fill_between(
                        d_arr, lower_bound, upper_bound,
                        color='#e67e22',    # same color as MA line
                        alpha=0.15,
                        label='MA ±1σ'
                    )

                # -------------------------------
                # 4. Calculate limits for y-axis to ensure visibility
                # -------------------------------
                if mvalues_arr is not None and mstds_arr is not None:
                    y_min = min(np.min(v_arr), np.min(lower_bound))
                    y_max = max(np.max(v_arr), np.max(upper_bound))
                else:
                    y_min = np.min(v_arr)
                    y_max = np.max(v_arr)


            # --- COMMON FORMATTING ---
            ax.set_title(title, fontsize=10, fontweight='bold', color='#333')
            ax.set_ylabel(y_label, fontsize=9)
            ax.grid(True, which='major', linestyle='--', alpha=0.6)
            ax.grid(True, which='minor', linestyle=':', alpha=0.3)

            # Set Y-Limits with 10% padding so band doesn't touch the frame
            span = y_max - y_min
            if span == 0:
                span = 1.0 if y_max == 0 else y_max * 0.1

            limit_min = y_min - (span * 0.1)
            if clamp_bottom and limit_min < 0:
                limit_min = 0

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



    def _moving_avg(self, y, N=120):
        """
        Compute trailing moving average and standard deviation.

        Parameters
        ----------
        y : array-like
            Input time series values
        N : int
            Window size (default: 120 cycles ≈ 30 days)

        Returns
        -------
        my : np.ndarray
            Moving average
        std : np.ndarray
            Moving standard deviation
        """
        y = np.asarray(y, dtype=float)

        my = np.full_like(y, np.nan)
        std = np.full_like(y, np.nan)

        for i in range(len(y)):
            start = max(0, i - N + 1)
            window = y[start:i + 1]

            my[i] = np.mean(window)
            std[i] = np.std(window)

        return my, std


    def _compute_marker_size(self, n_points, min_size=5, max_size=100):
        """
        Adaptive marker size for scatter plots.
        
        n_points: number of observations
        min_size: smallest marker (for huge datasets)
        max_size: largest marker (for tiny datasets)
        """
        if n_points <= 0:
            return max_size
        # log scaling: bigger n_points → smaller size
        size = max_size / (1 + np.log10(n_points))
        # clamp to min_size
        return max(min_size, size)

    def generate_surface_map(
        self,
        plot_path,
        plot_data
    ):
        self.legacy_generate_surface_map(
            plot_path,
            plot_data["dataset_name"],
            plot_data["obs_space_name"],
            plot_data["lats"],
            plot_data["lons"],
            plot_data["values"],
            plot_data["variable_name"],
            plot_data["units"]
        )

    def legacy_generate_surface_map(
        self, output_path, run_type, space,
        lats, lons, values, var_name, units="Units"
    ):
        # 1. Fixed figure size (all plots same size)
        fig = plt.figure(figsize=(12, 6))
        ax = plt.axes(projection=ccrs.Robinson())
        ax.set_global()  # entire globe
        ax.coastlines(resolution='110m', linewidth=0.5)
        ax.add_feature(cfeature.LAND, facecolor='lightgray', zorder=0)
        ax.add_feature(cfeature.OCEAN, facecolor='white', zorder=0)
        ax.gridlines(draw_labels=False)

        # 2. Subsample if too many points (to keep plot fast)
        lats, lons, values, _ = subsample_surface_points(
            lats, lons, values, max_points=300_000
        )

        # 3. Adaptive marker size
        n_points = len(values)
        marker_size = self._compute_marker_size(n_points)

        # 4. Determine color scale
        vmin, vmax, cmap = self.color_manager.resolve(values, var_name)

        # 5. Scatter plot
        sc = ax.scatter(
            lons, lats,
            c=values,
            s=marker_size,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            transform=ccrs.PlateCarree(),
            edgecolor='k',
            linewidth=0.2
        )

        # 6. Title
        ax.set_title(f"{run_type} - {space}", fontsize=14)

        # 7. Colorbar
        cbar = plt.colorbar(sc, ax=ax, orientation='vertical', pad=0.02)
        cbar.set_label(units)

        # 8. Save figure
        plt.savefig(output_path, bbox_inches='tight', dpi=150)
        plt.close(fig)

    def generate_interactive_surface_map(self, output_path, plot_data):
        """
        Generates an interactive HTML globe using Plotly.
        Mimics the subsampling and color scaling logic of the static map.
        """
        import plotly.graph_objects as go

        # 1. Extract data from plot_data dict
        lats = plot_data["lats"]
        lons = plot_data["lons"]
        values = plot_data["values"]
        var_name = plot_data["variable_name"]
        run_type = plot_data["dataset_name"]
        space = plot_data["obs_space_name"]
        units = plot_data.get("units", "Units")

        # 2. Replicate Subsampling logic (keeps performance fast)
        lats, lons, values, _ = subsample_surface_points(
            lats, lons, values, max_points=300_000
        )

        # 3. Replicate Color Scaling logic
        vmin, vmax, cmap_name = self.color_manager.resolve(values, var_name)
        
        # Note: Plotly uses different names for cmaps, but most standard 
        # Matplotlib names (Viridis, RdBu) work directly.
        
        # 4. Build the Plotly Figure
        fig = go.Figure(data=go.Scattergeo(
            lat=lats,
            lon=lons,
            mode='markers',
            marker=dict(
                size=3, # Fixed size usually works best for interactive
                color=values,
                colorscale=cmap_name,
                cmin=vmin,
                cmax=vmax,
                showscale=True,
                colorbar=dict(title=units, thickness=15),
                line=dict(width=0.1, color='black') # Mimics edgecolor='k'
            ),
            # Add hover information
            text=[f"{v:.2f} {units}" for v in values],
            hovertemplate="<b>Lat:</b> %{lat}<br><b>Lon:</b> %{lon}<br><b>Val:</b> %{text}<extra></extra>"
        ))

        # 5. Set Layout (Mimics Robinson projection with a Globe)
        fig.update_layout(
            title=f"Interactive Viewer: {run_type} - {space}",
            geo=dict(
                projection_type='orthographic', # Creates the interactive globe
                showland=True,
                landcolor="lightgray",
                showocean=True,
                oceancolor="white",
                showcoastlines=True,
                coastlinecolor="gray",
                coastlinewidth=0.5,
                center=dict(lat=0, lon=0),
                projection_scale=1,
            ),
            margin=dict(l=0, r=0, t=40, b=0),
            height=600
        )

        # 6. Save as standalone HTML
        fig.write_html(output_path)
        logger.info(f"Interactive HTML generated: {output_path}")


    def generate_history_plot_pd(
        self,
        df: pd.DataFrame,
        val_col: str,
        std_col: Optional[str],
        title: str,
        y_label: str,
        out_path: str,
        days: Optional[int] = None,
        clamp_bottom: bool = True,
        use_moving_avg: bool = True,
        window_size: int = 120  # 30 days if 4 cycles/day
    ):
        if not HAS_MATPLOTLIB or df.empty:
            return None

        # 1. Windowing (Last N days)
        if days is not None:
            cutoff = df.index.max() - pd.Timedelta(days=days)
            df = df[df.index >= cutoff]

        # 2. Extract Series
        v_arr = df[val_col]
        d_arr = df.index
        
        # 3. Initialize Plot
        fig, ax = plt.subplots(figsize=(10, 4))
        
        # MODE 1: Spatial Variance (from DB stddev column)
        if std_col and std_col in df.columns:
            s_arr = df[std_col]
            lower = v_arr - s_arr
            upper = v_arr + s_arr
            if clamp_bottom:
                lower = lower.clip(lower=0)
            
            ax.fill_between(d_arr, lower, upper, color='#3498db', alpha=0.3, label='±1σ (Spatial)')
            ax.plot(d_arr, v_arr, color='#2980b9', lw=2, label='Mean', marker='.', ms=4)
            
            y_min, y_max = lower.min(), upper.max()

        # MODE 2: Moving Average (Temporal Variance)
        elif use_moving_avg:
            # Vectorized Pandas Rolling Mean/Std
            roll = v_arr.rolling(window=window_size, min_periods=1)
            m_val = roll.mean()
            m_std = roll.std()
            
            lower = m_val - m_std
            if clamp_bottom:
                lower = lower.clip(lower=0)
            
            # Plot raw values in light blue, Trend in Orange
            ax.plot(d_arr, v_arr, color='#2980b9', alpha=0.3, lw=1, label='Actual')
            ax.plot(d_arr, m_val, color='#e67e22', lw=2, label='Moving Avg')
            ax.fill_between(d_arr, lower, m_val + m_std, color='#e67e22', alpha=0.15, label='MA ±1σ')
            
            y_min = min(v_arr.min(), lower.min())
            y_max = max(v_arr.max(), (m_val + m_std).max())

        # 4. Common Formatting (Reusing your logic)
        self._apply_styling(ax, title, y_label, d_arr, y_min, y_max, clamp_bottom)

        plt.tight_layout()
        plt.savefig(out_path, dpi=100)
        plt.close(fig)
        return os.path.basename(out_path)

    def _apply_styling(self, ax, title, y_label, d_arr, y_min, y_max, clamp_bottom):
        """Internal helper to standardize plot appearance."""
        ax.set_title(title, fontsize=10, fontweight='bold', color='#333')
        ax.set_ylabel(y_label, fontsize=9)
        ax.grid(True, which='major', linestyle='--', alpha=0.6)
        ax.grid(True, which='minor', linestyle=':', alpha=0.3)

        # Set Y-Limits with 10% padding
        span = y_max - y_min
        if span == 0:
            span = 1.0 if y_max == 0 else abs(y_max) * 0.1

        limit_min = y_min - (span * 0.1)
        if clamp_bottom and limit_min < 0:
            limit_min = 0

        ax.set_ylim(limit_min, y_max + (span * 0.1))

        # Date Axis Formatting - Native Pandas handling
        if len(d_arr) > 0:
            # If the span of dates is large, show days; if small, show hours
            date_range = d_arr.max() - d_arr.min()
            if date_range.days > 2:
                ax.xaxis.set_major_locator(mdates.AutoDateLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

        # Standard Matplotlib rotation/cleanup
        plt.gcf().autofmt_xdate()
        ax.legend(loc='upper left', fontsize='small', frameon=True)
