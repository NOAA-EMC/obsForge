import os

class WebsiteStructure:
    def __init__(self, output_dir, run_types):
        self.output_dir = os.path.abspath(output_dir)
        self.run_types = run_types
        self.runs_dir = os.path.join(self.output_dir, "runs")

    # ---- Path helpers ----

    def run_root(self, rt):
        return os.path.join(self.runs_dir, rt)

    def plots_dir(self, rt):
        return os.path.join(self.run_root(rt), "plots")

    def categories_dir(self, rt):
        return os.path.join(self.run_root(rt), "categories")

    def obsspaces_dir(self, rt):
        return os.path.join(self.run_root(rt), "observations")

    # def cycles_dir(self, rt):
        # return os.path.join(self.run_root(rt), "cycles")

    # ---- Directory creation ----

    def create(self):
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.runs_dir, exist_ok=True)

        for rt in self.run_types:
            for d in (
                self.run_root(rt),
                self.plots_dir(rt),
                self.categories_dir(rt),
                self.obsspaces_dir(rt),
                # self.cycles_dir(rt),
            ):
                os.makedirs(d, exist_ok=True)
