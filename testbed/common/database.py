"""PostgreSQL tables isolated in the ``testbed`` schema."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from app.config import get_settings


class TestbedBase(DeclarativeBase):
    pass


class User(TestbedBase):
    __tablename__ = "users"
    __table_args__ = {"schema": "testbed"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)


class Order(TestbedBase):
    __tablename__ = "orders"
    __table_args__ = {"schema": "testbed"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("testbed.users.id"), nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


def create_testbed_engine():
    url = get_settings().database_url
    return create_engine(url, pool_pre_ping=True, connect_args={"options": "-csearch_path=testbed"})


def initialize_schema() -> None:
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS testbed"))
        TestbedBase.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO testbed.users (id, name) VALUES "
                     "(1, 'Avery Chen'), (2, 'Jordan Patel'), (3, 'Morgan Rivera') "
                     "ON CONFLICT (id) DO NOTHING")
            )
    finally:
        engine.dispose()


TestbedSession = sessionmaker(bind=create_testbed_engine(), expire_on_commit=False)