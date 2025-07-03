"""
Core enterprise architecture per ANAC Importer.
Implementa Domain Layer, Repository Pattern, Service Layer secondo principi SOLID.
"""

from .domain import ANACRecord, ImportJob, ValidationResult
from .repository import ANACRepository, TransactionManager
from .services import ImportService, ValidationService, IntegrityService
from .exceptions import ImportError, ValidationError, IntegrityError

__all__ = [
    'ANACRecord',
    'ImportJob', 
    'ValidationResult',
    'ANACRepository',
    'TransactionManager',
    'ImportService',
    'ValidationService',
    'IntegrityService',
    'ImportError',
    'ValidationError', 
    'IntegrityError'
]