from collections import defaultdict
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta

from .models import CycleData


class CoverageAnalyzer:
    def __init__(self, cycles: List[CycleData]):
        self.cycles = cycles

    @staticmethod
    def cycle_to_datetime(date: str, cycle: int) -> datetime:
        return datetime.strptime(date, "%Y%m%d") + timedelta(hours=cycle)

    @staticmethod
    def datetime_to_cycle_key(dt: datetime) -> str:
        return f"{dt.strftime('%Y%m%d')}_{dt.hour:02d}"

    def condensed_missing_obs_space_report(self, cycle_spacing_hours: int = 6) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
        """
        Returns a condensed report of missing obs spaces:
        {
            run_type: {
                obs_space_name: [
                    (first_missing_cycle, last_missing_cycle),
                    ...
                ]
            }
        }
        Only obs spaces with missing cycles are included.
        """

        # Step 1: Compile all obs spaces per run type
        run_type_obs_spaces = defaultdict(set)
        for c in self.cycles:
            for task in c.tasks:
                for f in task.files:
                    run_type_obs_spaces[task.run_type].add(f.obs_space_name)

        # Step 2: Track missing cycles per obs space
        missing_map = defaultdict(lambda: defaultdict(list))  # run_type -> obs_space -> list of cycle datetimes

        for c in self.cycles:
            for task in c.tasks:
                run_type = task.run_type
                all_obs_spaces = run_type_obs_spaces[run_type]
                present_obs_spaces = {f.obs_space_name for f in task.files}
                missing_obs = all_obs_spaces - present_obs_spaces
                for obs in missing_obs:
                    missing_map[run_type][obs].append(self.cycle_to_datetime(c.date, c.cycle))

        # Step 3: Condense consecutive cycles into ranges
        condensed_report = defaultdict(dict)

        for run_type, obs_dict in missing_map.items():
            for obs, dt_list in obs_dict.items():
                if not dt_list:
                    continue
                dt_list.sort()
                ranges = []
                start = dt_list[0]
                end = dt_list[0]

                for dt in dt_list[1:]:
                    if dt == end + timedelta(hours=cycle_spacing_hours):
                        end = dt
                    else:
                        ranges.append((self.datetime_to_cycle_key(start), self.datetime_to_cycle_key(end)))
                        start = dt
                        end = dt
                ranges.append((self.datetime_to_cycle_key(start), self.datetime_to_cycle_key(end)))
                condensed_report[run_type][obs] = ranges

        return condensed_report
