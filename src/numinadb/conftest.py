import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .model import Base


@pytest.fixture
def db_session():
    database = "sqlite:///:memory:"
    engine = create_engine(database, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    try:
        with Session() as session:
            yield session
    finally:
        Base.metadata.drop_all(engine)
