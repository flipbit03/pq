"""SQLAlchemy 2.0 models for PQ task queue."""

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    Identity,
    Index,
    Interval,
    SmallInteger,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class Task(Base):
    """One-off task (deleted after execution)."""

    __tablename__ = "pq_tasks"
    __table_args__ = (Index("ix_pq_tasks_priority_run_at", "priority", "run_at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Periodic(Base):
    """Recurring task."""

    __tablename__ = "pq_periodic"
    __table_args__ = (
        Index("ix_pq_periodic_priority_next_run", "priority", "next_run"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    run_every: Mapped[timedelta] = mapped_column(Interval, nullable=False)
    next_run: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_run: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
