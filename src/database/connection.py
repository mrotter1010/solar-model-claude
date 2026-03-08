"""Database engine and session factory."""

import os
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

DEFAULT_DATABASE_URL = (
    "postgresql://solar_model:solar_model@localhost:5432/solar_model"
)


def get_engine(url: str | None = None) -> Engine:
    """Create a SQLAlchemy engine.

    Args:
        url: Database URL. Falls back to DATABASE_URL env var, then default.
    """
    database_url = url or os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    return create_engine(database_url)


def get_session_factory(url: str | None = None) -> sessionmaker[Session]:
    """Create a bound sessionmaker.

    Args:
        url: Database URL. Falls back to DATABASE_URL env var, then default.
    """
    engine = get_engine(url)
    return sessionmaker(bind=engine)


@contextmanager
def get_session(url: str | None = None) -> Generator[Session, None, None]:
    """Provide a transactional database session.

    Commits on success, rolls back on exception, always closes.

    Args:
        url: Database URL. Falls back to DATABASE_URL env var, then default.
    """
    factory = get_session_factory(url)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
