import logging

from .variable import Variable

logger = logging.getLogger(__name__)


class ObsSpace:
    """
    API wrapper over ds.Field.
    """

    def __init__(self, field, repo):
        self._field = field
        self._repo = repo

        # cache structure
        self._structure = field.obs_space.netcdf_structure

        self._repo.load_field_files(self._field)

    def __repr__(self):
        return f"<ObsSpace {self.name}>"

    @property
    def name(self):
        return self._field.obs_space.name

    def variable(self, name: str) -> Variable:
        """
        Resolve a variable by name or path.

        Examples:
            "lon"
            "ObsValue/seaSurfaceTemperature"
        """
        structure = self._field.obs_space.netcdf_structure

        # --- full path ---
        if "/" in name:
            path = structure.check_variable_path(name)
            if path:
                return Variable(self._field, path, self._repo)
            else:
                # logger.error(f"{self.name}:{name} not found")
                raise ValueError(f"Variable '{name}' not found")

        # --- short name ---
        paths = structure.find_variable_paths_by_name(name)

        if not paths:
            raise ValueError(f"Variable '{name}' not found")

        if len(paths) > 1:
            raise ValueError(f"Variable '{name}' is ambiguous. Matches: {paths}")

        return Variable(self._field, paths[0], self._repo)

    '''
    def list_variables(self):
        return self._field.obs_space.netcdf_structure.list_variable_paths()
    '''
