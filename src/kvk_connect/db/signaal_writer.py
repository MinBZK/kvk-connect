# ruff: noqa: D102
import logging

from sqlalchemy.orm import Session, sessionmaker

from kvk_connect.models.api.mutatiesignalen_api import MutatieSignaal
from kvk_connect.models.orm.signaal_orm import SignaalORM

logger = logging.getLogger(__name__)


class SignaalWriter:
    def __init__(self, engine, batch_size: int = 10, upsert: bool = True):
        logger.info("Initializing BasisProfielWriter, met batch size: %d", batch_size)
        self.Session = sessionmaker(bind=engine)
        self.batch_size = batch_size
        self.upsert = upsert
        self._session: Session | None = None
        self._buffer: list[SignaalORM] = []
        self._count = 0

    def __enter__(self):
        """Create a new session on entry of context manager."""
        self._session = self.Session()
        return self

    def __exit__(self, exc_type, exc, tb):
        """Handle commit/rollback on exit of context manager."""
        try:
            if exc is None:
                self.flush()
            else:
                if self._session:
                    self._session.rollback()
        finally:
            if self._session:
                self._session.close()
            self._session = None

    def add(self, api_signaal: MutatieSignaal) -> None:
        if not self._session:
            raise RuntimeError("Session not initialized. Use context manager.")

        orm_obj = self._to_orm(api_signaal)
        if self.upsert:
            self._session.merge(orm_obj)  # upsert per row
            self._count += 1
            if self._count % self.batch_size == 0:
                self._session.commit()
        else:
            self._buffer.append(orm_obj)
            if len(self._buffer) >= self.batch_size:
                self._session.bulk_save_objects(self._buffer)
                self._session.commit()
                self._buffer.clear()

    def flush(self) -> None:
        if not self._session:
            return
        if not self.upsert and self._buffer:
            self._session.bulk_save_objects(self._buffer)
            self._buffer.clear()
        self._session.commit()

    @staticmethod
    def _to_orm(s: MutatieSignaal) -> SignaalORM:
        return SignaalORM(
            id=s.id,
            timestamp=s.timestamp,
            kvknummer=s.kvknummer,
            signaal_type=s.signaal_type,
            vestigingsnummer=s.vestigingsnummer,
        )
