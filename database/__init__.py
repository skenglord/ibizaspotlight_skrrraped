"""
MongoDB Event Data Quality System

This module provides tools for managing event data with quality scoring in MongoDB.
"""

from .mongodb_setup import MongoDBSetup
from .quality_scorer import QualityScorer
from .data_migration import DataMigration

__version__ = "1.0.0"
__all__ = ["MongoDBSetup", "QualityScorer", "DataMigration"]