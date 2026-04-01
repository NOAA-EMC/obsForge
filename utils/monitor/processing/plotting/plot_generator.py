import logging
import os
import shutil
from datetime import datetime
from datetime import timedelta

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
    import matplotlib.ticker as mticker

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
            out_path,
            clamp_bottom,
        )




    def generate_dual_plots(
        self, title, data, val_key, std_key, fname_base, y_label,
        clamp_bottom=True
    ):
        """
        Main entry point. Orchestrates data prep and calling the renderer twice.

        Args:
            title (str): Chart title.
            data (list): List of dicts from DataService.
            val_key (str): Key for the main line value (e.g., 'total_obs').
            std_key (str): Key for the band width (e.g., 'std_dev').
                           Pass None for Temporal Mode.
            fname_base (str): Output filename prefix.
            y_label (str): Y-axis label.
            clamp_bottom (bool): If True, bands stop at 0 (for Counts).
                                 False for Physics (e.g. Temp).
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
                if v is None:
                    continue

                # If std_key is provided, extract it.
                # Else None (triggers Temporal Mode).
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
        full_path = os.path.join(self.output_dir, f_full)
        self._plot_series(
            dates, values, stds, title, y_label, full_path, clamp_bottom
        )

        # 4. Generate 7-Day Zoom Plot
        f_7d = f"{fname_base}_7d.png"
        zoom_path = os.path.join(self.output_dir, f_7d)

        # Slice last 28 points (approx 7 days @ 4 cycles/day)
        cutoff = -28 if len(dates) > 28 else 0

        if cutoff != 0:
            self._plot_series(
                dates[cutoff:], values[cutoff:], stds[cutoff:],
                title, y_label, zoom_path, clamp_bottom
            )
        else:
            # If dataset is small, zoom plot is identical to full plot
            shutil.copy(full_path, zoom_path)

        return f_full, f_7d

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
            '''
            if len(d_arr) > 14:
                ax.xaxis.set_major_locator(mdates.DayLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            '''

            # Date Axis Formatting (adaptive, prevents congestion)
            n = len(d_arr)

            if n > 100:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

            elif n > 50:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

            elif n > 20:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

            else:
                ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%H'))


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

                '''
                # Apply clamp_bottom if requested
                if clamp_bottom is not None and y_min < clamp_bottom:
                    y_min = clamp_bottom
                ax.set_ylim(y_min, y_max)

                # Labels, title, grid, legend
                ax.set_xlabel("Date")
                ax.set_ylabel(y_label)
                ax.set_title(title)
                ax.grid(True)
                ax.legend()

                if out_path:
                    plt.savefig(out_path, bbox_inches="tight")
                plt.close(fig)
                '''


                '''
                # global_mean = np.mean(v_arr)
                # global_std = np.std(v_arr)

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
                '''

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

            '''
            # Date Axis Formatting
            if len(d_arr) > 14:
                ax.xaxis.set_major_locator(mdates.DayLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            '''

            # Date Axis Formatting (adaptive, prevents congestion)
            n = len(d_arr)

            if n > 100:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

            elif n > 50:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

            elif n > 20:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

            else:
                ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%H'))



            fig.autofmt_xdate()
            ax.legend(loc='upper left', fontsize='small', frameon=True)

            plt.tight_layout()
            plt.savefig(out_path, dpi=100)
            plt.close(fig)
            return True

        except Exception as e:
            logger.error(f"Render failed for {out_path}: {e}")
            return False







    def try_plot_series_with_moving_avg(
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
        """
        Generic plotting function for time series / spatial series.

        Arguments:
            dates      : list of datetime objects or x-axis values
            values     : list of y-values (original)
            mvalues    : list of moving average values (optional)
            mstds      : list of moving std deviations (optional)
            stds       : standard deviation of original values (optional)
            title      : plot title
            y_label    : label for y-axis
            out_path   : path to save the figure (optional)
            clamp_bottom: minimum y-value (optional)
        """
        import matplotlib.pyplot as plt
        import numpy as np

        # ------------------------------------------------------------------
        # Detect if data is spatial or temporal
        # ------------------------------------------------------------------
        has_spatial_std = False
        if isinstance(values, (list, np.ndarray)) and len(values) > 0:
            if hasattr(values[0], "__len__"):  # array of arrays
                has_spatial_std = True

        if has_spatial_std:
            # ------------------------------------------------------------------
            # MODE 1: SPATIAL VARIANCE
            # Original spatial plotting code remains unchanged
            # ------------------------------------------------------------------
            fig, ax = plt.subplots(figsize=(10, 5))
            values_arr = np.array(values)
            mean_val = np.mean(values_arr, axis=1)
            std_val = np.std(values_arr, axis=1)

            ax.plot(dates, mean_val, color='#2980b9', linewidth=2, label='Mean Value')
            if std_val is not None:
                ax.fill_between(dates, mean_val - std_val, mean_val + std_val,
                                color='#2980b9', alpha=0.1, label='±1σ')

            ax.set_xlabel("Date")
            ax.set_ylabel(y_label)
            ax.set_title(title)
            ax.grid(True)
            ax.legend()

            if out_path:
                plt.savefig(out_path, bbox_inches="tight")
            plt.close(fig)

        else:
            # ------------------------------------------------------------------
            # MODE 2: TEMPORAL VARIANCE (HISTORICAL) WITH MOVING AVERAGE
            # The band represents variability of the moving average over time.
            # ------------------------------------------------------------------
            fig, ax = plt.subplots(figsize=(10, 5))

            # Convert to numpy arrays for calculations
            v_arr = np.array(values)
            d_arr = np.array(dates)
            mvalues_arr = np.array(mvalues) if mvalues is not None else None
            mstds_arr = np.array(mstds) if mstds is not None else None

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

            # Apply clamp_bottom if requested
            if clamp_bottom is not None and y_min < clamp_bottom:
                y_min = clamp_bottom
            ax.set_ylim(y_min, y_max)

            # Labels, title, grid, legend
            ax.set_xlabel("Date")
            ax.set_ylabel(y_label)
            ax.set_title(title)
            ax.grid(True)
            ax.legend()

            if out_path:
                plt.savefig(out_path, bbox_inches="tight")
            plt.close(fig)


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

    '''
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
    '''

    def generate_NOAA_Obs_count_plot(
        self,
        title,
        data,
        val_key,
        fname,
        y_label="Obs per Hour",
    ):
        """
        NOAA-style observation count plot:
        - Green dashed line: hourly obs (cycle value / 6)
        - Red solid line: 30-day (~120 cycles) moving avg
        - Visible markers at each cycle
        - 3-day window (last 12 cycles)
        - Black grid (vertical per cycle, horizontal dynamic)
        - X labels: dd/hh
        """

        if not HAS_MATPLOTLIB or not data:
            return None

        dates = []
        values = []

        # -------------------------
        # Parse data
        # -------------------------
        for r in data:
            try:
                dt_str = f"{r['date']}{r['cycle']:02d}"
                dt = datetime.strptime(dt_str, "%Y%m%d%H")

                v = r.get(val_key)
                if v is None:
                    continue

                # ✅ Convert to obs per hour
                v = v / 6.0

                dates.append(dt)
                values.append(v)

            except Exception as e:
                logger.error(f"Bad record skipped: {r} ({e})")

        if not dates:
            return None
        # print("RAW VALUES:", values[:10])

        # -------------------------
        # Compute 30-day moving avg (~120 cycles)
        # -------------------------
        # mvalues, _ = self._moving_avg(v_arr, N=120)

        full_v_arr = np.array(values)
        mvalues_full, _ = self._moving_avg(full_v_arr, N=120)


        # -------------------------
        # Build fixed 3-day timeline (12 cycles)
        # -------------------------
        end_time = dates[-1]

        # Align to nearest 6-hour cycle (optional but recommended)
        end_time = end_time.replace(hour=(end_time.hour // 6) * 6)

        time_grid = [end_time - timedelta(hours=6 * i) for i in range(12)]
        time_grid = list(reversed(time_grid))  # oldest → newest

        # -------------------------
        # Map existing data onto grid
        # -------------------------
        data_dict = {d: v for d, v in zip(dates, full_v_arr)}
        ma_dict = {d: v for d, v in zip(dates, mvalues_full)}

        d_arr = np.array(time_grid)

        v_arr = np.array([
            data_dict.get(t, np.nan) for t in time_grid
        ])

        mvalues = np.array([
            ma_dict.get(t, np.nan) for t in time_grid
        ])

        '''
        # -------------------------
        # Restrict to last 3 days (12 cycles)
        # -------------------------
        points = 3 * 4
        # if len(dates) > points:
            # dates = dates[-points:]
            # values = values[-points:]

        dates = dates[-points:]
        v_arr = full_v_arr[-points:]
        mvalues = mvalues_full[-points:]

        d_arr = np.array(dates)
        # v_arr = np.array(values)

        print("len(d_arr):", len(d_arr))
        print("date range:", d_arr[0], "→", d_arr[-1])
        '''

        # -------------------------
        # Output path
        # -------------------------
        out_path = os.path.join(self.output_dir, fname)

        try:
            fig, ax = plt.subplots(figsize=(12, 5))

            # -------------------------
            # Plot lines
            # -------------------------
            ax.plot(
                d_arr,
                v_arr,
                color='green',
                linestyle='--',
                linewidth=2,
                marker='o',
                markersize=4,
                label='Hourly Obs'
            )

            ax.plot(
                d_arr,
                mvalues,
                color='red',
                linestyle='-',
                linewidth=2,
                marker='o',
                markersize=4,
                label='30-Day Avg'
            )


            # -------------------------
            # Grid styling
            # -------------------------

            # --- axis ticks only (no grid) ---
            ax.xaxis.set_major_locator(mdates.DayLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%H'))

            ax.xaxis.set_minor_locator(mdates.HourLocator(interval=6))

            # disable auto grid so it doesn't conflict
            ax.grid(False)

            # Vertical lines per cycle (OK for 3-day window)
            # for dt in d_arr:
                # ax.axvline(dt, color='black', linewidth=0.5, alpha=0.6)

            for dt in d_arr:
                if dt.hour == 18:
                    # ✅ Day boundary (bold)
                    ax.axvline(dt, color='black', linewidth=1.5, alpha=0.9)
                else:
                    # regular cycle lines
                    ax.axvline(dt, color='black', linewidth=0.5, alpha=0.5)

            # -------------------------
            # Y-axis scaling
            # -------------------------
            y_min = 0
            y_max = np.max(v_arr)

            span = y_max - y_min

            # ✅ Improved dv logic (handles small values properly)
            if span <= 10:
                dv = 1
            elif span <= 50:
                dv = 5
            elif span <= 200:
                dv = 10
            elif span <= 2000:
                dv = 200
            elif span <= 20000:
                dv = 1000
            elif span <= 200000:
                dv = 10000
            else:
                dv = int(span / 10)

            # Ensure at least a few ticks exist
            if dv == 0:
                dv = 1

            yticks = np.arange(y_min, y_max + dv, dv)
            ax.set_yticks(yticks)

            # ✅ Disable scientific notation + add commas
            ax.yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))

            '''
            # -------------------------
            # Grid styling
            # -------------------------
            # Vertical lines per cycle
            for dt in d_arr:
                ax.axvline(dt, color='black', linewidth=0.5, alpha=0.6)

            # Horizontal spacing
            y_min = 0
            y_max = np.max(v_arr)

            span = y_max - y_min
            if span <= 2000:
                dv = 200
            elif span <= 20000:
                dv = 1000
            elif span <= 200000:
                dv = 10000
            else:
                dv = int(span / 10)

            yticks = np.arange(y_min, y_max + dv, dv)
            ax.set_yticks(yticks)
            '''

            for y in yticks:
                ax.axhline(y, color='black', linewidth=0.5, alpha=0.6)

            # -------------------------
            # Axis formatting
            # -------------------------
            ax.set_xlim(d_arr[0], d_arr[-1])
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%H'))

            ax.set_ylabel(y_label)
            ax.set_title(title, fontsize=11, fontweight='bold')

            # -------------------------
            # NOAA label
            # -------------------------
            ax.text(
                0.01, 0.95,
                "NOAA",
                transform=ax.transAxes,
                fontsize=14,
                fontweight='bold',
                color='navy',
                alpha=0.7,
                verticalalignment='top'
            )

            # -------------------------
            # Limits
            # -------------------------
            ax.set_ylim(0, y_max * 1.1)

            # -------------------------
            # Legend
            # -------------------------
            ax.legend(loc='upper left', fontsize='small', frameon=True)

            plt.tight_layout()
            plt.savefig(out_path, dpi=120)
            plt.close(fig)

            return fname

        except Exception as e:
            logger.error(f"NOAA plot failed for {fname}: {e}")
            return None
