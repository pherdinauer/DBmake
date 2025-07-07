"""
Eccezioni custom per l'architettura enterprise ANAC Importer.
Fornisce gestione strutturata degli errori con categorizzazione e recovery.
"""

from typing import Optional, Dict, Any
from enum import Enum

class ErrorSeverity(Enum):
    """Livelli di severità degli errori."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    """Categorie di errori."""
    SECURITY = "security"
    DATA_INTEGRITY = "data_integrity"
    VALIDATION = "validation"
    DATABASE = "database"
    NETWORK = "network"
    CONFIGURATION = "configuration"
    BUSINESS_LOGIC = "business_logic"

class ANACImporterError(Exception):
    """Eccezione base per ANAC Importer."""
    
    def __init__(self, 
                 message: str,
                 error_code: str = None,
                 severity: ErrorSeverity = ErrorSeverity.MEDIUM,
                 category: ErrorCategory = ErrorCategory.BUSINESS_LOGIC,
                 context: Dict[str, Any] = None,
                 recoverable: bool = True):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.severity = severity
        self.category = category
        self.context = context or {}
        self.recoverable = recoverable
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte l'eccezione in dizionario per logging strutturato."""
        return {
            'error_type': self.__class__.__name__,
            'error_code': self.error_code,
            'message': self.message,
            'severity': self.severity.value,
            'category': self.category.value,
            'recoverable': self.recoverable,
            'context': self.context
        }

class SecurityError(ANACImporterError):
    """Errori di sicurezza - massima priorità."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.SECURITY,
            recoverable=False,
            **kwargs
        )

class IntegrityError(ANACImporterError):
    """Errori di integrità dei dati - critico per zero data loss."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.DATA_INTEGRITY,
            recoverable=False,
            **kwargs
        )

class ValidationError(ANACImporterError):
    """Errori di validazione - recuperabili con sanitizzazione."""
    
    def __init__(self, message: str, field_name: str = None, **kwargs):
        context = kwargs.get('context', {})
        if field_name:
            context['field_name'] = field_name
        kwargs['context'] = context
        
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.VALIDATION,
            recoverable=True,
            **kwargs
        )

class DatabaseError(ANACImporterError):
    """Errori del database - possono richiedere retry."""
    
    def __init__(self, message: str, operation: str = None, **kwargs):
        context = kwargs.get('context', {})
        if operation:
            context['operation'] = operation
        kwargs['context'] = context
        
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.DATABASE,
            recoverable=True,
            **kwargs
        )

class ConfigurationError(ANACImporterError):
    """Errori di configurazione - richiedono intervento admin."""
    
    def __init__(self, message: str, config_key: str = None, **kwargs):
        context = kwargs.get('context', {})
        if config_key:
            context['config_key'] = config_key
        kwargs['context'] = context
        
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.CONFIGURATION,
            recoverable=False,
            **kwargs
        )

class ImportError(ANACImporterError):
    """Errori durante l'importazione - con contesto del job."""
    
    def __init__(self, message: str, job_id: str = None, batch_id: str = None, **kwargs):
        context = kwargs.get('context', {})
        if job_id:
            context['job_id'] = job_id
        if batch_id:
            context['batch_id'] = batch_id
        kwargs['context'] = context
        
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.BUSINESS_LOGIC,
            **kwargs
        )

class TransactionError(DatabaseError):
    """Errori specifici delle transazioni ACID."""
    
    def __init__(self, message: str, transaction_id: str = None, **kwargs):
        context = kwargs.get('context', {})
        if transaction_id:
            context['transaction_id'] = transaction_id
        kwargs['context'] = context
        
        super().__init__(
            message,
            operation="transaction",
            **kwargs
        )

class CredentialError(SecurityError):
    """Errori specifici delle credenziali."""
    
    def __init__(self, message: str, credential_type: str = None, **kwargs):
        context = kwargs.get('context', {})
        if credential_type:
            context['credential_type'] = credential_type
        kwargs['context'] = context
        
        super().__init__(message, **kwargs)

class ChecksumMismatchError(IntegrityError):
    """Errore specifico per mismatch di checksum."""
    
    def __init__(self, expected: str, actual: str, source: str = None, **kwargs):
        message = f"Checksum mismatch: expected {expected}, got {actual}"
        if source:
            message += f" in {source}"
        
        context = kwargs.get('context', {})
        context.update({
            'expected_checksum': expected,
            'actual_checksum': actual,
            'source': source
        })
        kwargs['context'] = context
        
        super().__init__(message, **kwargs)