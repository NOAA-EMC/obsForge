class Dataset:
    """
    High-level domain object representing a dataset.

    This class:
        - Owns a DatasetService instance
        - Delegates persistence to the service
        - Contains no SQL
    """

    def __init__(self, db_path: str, name: str):
        """
        Create a Dataset domain object.

        Parameters
        ----------
        db_path : str
            Path to SQLite database.
        name : str
            Run type name (e.g. 'gfs').
        """

        self.name = name

        # Service is owned internally.
        # Dataset controls persistence through it.
        # from database.dataset_service import DatasetService
        from .dataset_db import DatasetService
        self._service = DatasetService(db_path)

        # Ensure dataset exists in DB and cache its ID.
        self._id = self._service.ensure_dataset(name)

    # ------------------------------------------------------------------

    def add_cycle(self, cycle_date: str, cycle_hour: str) -> None:
        """
        Register a cycle (date + hour) for this dataset.

        Delegates persistence to DatasetService.
        """

        self._service.ensure_cycle(
            dataset_id=self._id,
            cycle_date=cycle_date,
            cycle_hour=cycle_hour
        )

    # ------------------------------------------------------------------

    @property
    def id(self) -> int:
        return self._id

    @property
    def dataset_name(self) -> str:
        return self.name

