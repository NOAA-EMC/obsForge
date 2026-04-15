import os

class WebsiteStructure:
    def __init__(self, output_dir, run_types):
        self.output_dir = os.path.abspath(output_dir)
        self.run_types = run_types
        self.html_dir = os.path.join(self.output_dir, "html")

    # ---- Path helpers ----

    def run_root(self, rt):
        return os.path.join(self.html_dir, rt)

    def categories_dir(self, rt):
        return os.path.join(self.run_root(rt), "categories")

    def obsspaces_dir(self, rt):
        return os.path.join(self.run_root(rt), "obs_spaces")

    # ---- Directory creation ----

    def create(self):
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.html_dir, exist_ok=True)

        for rt in self.run_types:
            for d in (
                self.run_root(rt),
                self.categories_dir(rt),
                self.obsspaces_dir(rt),
            ):
                os.makedirs(d, exist_ok=True)
