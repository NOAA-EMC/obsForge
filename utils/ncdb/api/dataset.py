import logging

from .obsspace import ObsSpace

logger = logging.getLogger(__name__)


class Dataset:
    def __init__(self, core_ds, repo):
        self._ds = core_ds
        self._repo = repo

        self._repo.load_fields(self._ds)
        if not self._ds.fields:
            logger.error(f"Failed loading fields for dataset '{self.name}'")

        self._cycles = None   # instead of []

    def __repr__(self):
        return f"<Dataset name={self._ds.name}>"

    @property
    def name(self):
        return self._ds.name

    @property
    def cycles(self):
        if self._cycles is None:
            self._repo.load_cycles(self._ds)
            self._cycles = self._ds.cycles
        return self._cycles

    def list_obsspaces(self) -> list[str]:
        return [f.obs_space.name for f in self._ds.fields]

    def obsspace(self, name: str) -> ObsSpace:
        field = self._ds.find_field_by_name(name)
        if not field:
            raise ValueError(f"ObsSpace '{name}' not found")

        return ObsSpace(field, self._repo)
