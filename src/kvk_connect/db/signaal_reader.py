from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from kvk_connect.models.orm.signaal_orm import SignaalORM


class SignaalReader:
    def __init__(self, engine):
        self.Session = sessionmaker(bind=engine)

    def get_last_timestamp(self) -> datetime | None:
        """Returns the latest stored signaal timestamp, or None if table is empty."""
        with self.Session() as session:
            stmt = select(func.max(SignaalORM.timestamp))
            return session.execute(stmt).scalar()

    def get_first_timestamp(self) -> datetime | None:
        """Returns the latest stored signaal timestamp, or None if table is empty."""
        with self.Session() as session:
            stmt = select(func.min(SignaalORM.timestamp))
            return session.execute(stmt).scalar()
