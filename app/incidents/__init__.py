"""
app/incidents package
=====================
Incident aggregation, life-cycle tracking, evidence pack creation.
"""
from app.incidents.evidence import build_evidence_pack
from app.incidents.manager import IncidentManager, IncidentRecord

__all__ = ["IncidentManager", "IncidentRecord", "build_evidence_pack"]
