"""
app/ingest package
==================
Log ingestion utilities, tailer, and shared batch processing.
"""
from app.ingest.batch import ingest_batch

__all__ = ["ingest_batch"]
