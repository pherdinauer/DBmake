"""
Validatore degli input per prevenire injection attacks e validare dati.
"""

import re
import html
from typing import Any, List, Dict, Optional, Union
import logging

logger = logging.getLogger(__name__)

class SQLInjectionValidator:
    """Validatore specifico per prevenire SQL injection attacks."""
    
    def __init__(self):
        # Pattern SQL pericolosi
        self.dangerous_patterns = [
            re.compile(r'\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|EXEC|EXECUTE)\b', re.IGNORECASE),
            re.compile(r'[\'\"]\s*;\s*[\'\"]*\s*(DROP|DELETE|INSERT|UPDATE|ALTER)', re.IGNORECASE),
            re.compile(r'UNION\s+SELECT', re.IGNORECASE),
            re.compile(r'[\'\"]\s*OR\s+[\'\"]*\d+[\'\"]*\s*=\s*[\'\"]*\d+', re.IGNORECASE),
            re.compile(r'[\'\"]\s*AND\s+[\'\"]*\d+[\'\"]*\s*=\s*[\'\"]*\d+', re.IGNORECASE),
            re.compile(r'[\'\"]\s*--', re.IGNORECASE),
            re.compile(r'/\*.*\*/', re.IGNORECASE),
        ]
    
    def is_safe(self, value: str) -> bool:
        """Verifica se il valore è sicuro da SQL injection."""
        if not isinstance(value, str):
            return True
            
        for pattern in self.dangerous_patterns:
            if pattern.search(value):
                logger.warning(f"Rilevato potenziale SQL injection: {value[:50]}...")
                return False
        return True
    
    def sanitize(self, value: str) -> str:
        """Sanitizza il valore rimuovendo caratteri pericolosi."""
        if not isinstance(value, str):
            return str(value)
        
        # Escape HTML
        sanitized = html.escape(value)
        
        # Rimuovi caratteri SQL pericolosi
        sanitized = re.sub(r'[;\'\"\\]', '', sanitized)
        
        return sanitized

class InputValidator:
    """Validatore completo degli input con regole specifiche per ANAC."""
    
    def __init__(self):
        self.sql_validator = SQLInjectionValidator()
        
        # Pattern per dati italiani specifici
        self.patterns = {
            'codice_fiscale': re.compile(r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$'),
            'partita_iva': re.compile(r'^\d{11}$'),
            'cig': re.compile(r'^[A-Z0-9]{10}$'),
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'telefono': re.compile(r'^(\+39\s?)?((0\d{1,4}\s?)?\d{6,10})$'),
        }
    
    def validate_field(self, field_name: str, value: Any, field_type: str = None) -> Dict[str, Any]:
        """
        Valida un singolo campo.
        
        Returns:
            Dict con 'is_valid', 'sanitized_value', 'errors'
        """
        result = {
            'is_valid': True,
            'sanitized_value': value,
            'errors': []
        }
        
        # Controllo SQL injection
        if isinstance(value, str) and not self.sql_validator.is_safe(value):
            result['is_valid'] = False
            result['errors'].append('Potenziale SQL injection rilevato')
            result['sanitized_value'] = self.sql_validator.sanitize(value)
        
        # Validazione specifica per tipo
        if field_type and isinstance(value, str):
            if field_type in self.patterns:
                if not self.patterns[field_type].match(value):
                    result['errors'].append(f'Formato {field_type} non valido')
        
        # Validazione specifica per campo
        if field_name and isinstance(value, str):
            field_lower = field_name.lower()
            
            if 'codice_fiscale' in field_lower or 'cf' in field_lower:
                if not self.patterns['codice_fiscale'].match(value.upper()):
                    result['errors'].append('Codice fiscale non valido')
                    
            elif 'partita_iva' in field_lower or 'piva' in field_lower:
                if not self.patterns['partita_iva'].match(value):
                    result['errors'].append('Partita IVA non valida')
                    
            elif 'cig' in field_lower:
                if not self.patterns['cig'].match(value.upper()):
                    result['errors'].append('CIG non valido')
                    
            elif 'email' in field_lower or 'mail' in field_lower:
                if not self.patterns['email'].match(value.lower()):
                    result['errors'].append('Email non valida')
        
        if result['errors']:
            result['is_valid'] = False
            
        return result
    
    def validate_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida un intero record.
        
        Returns:
            Dict con 'is_valid', 'sanitized_record', 'field_errors'
        """
        result = {
            'is_valid': True,
            'sanitized_record': {},
            'field_errors': {}
        }
        
        for field_name, value in record.items():
            validation = self.validate_field(field_name, value)
            
            result['sanitized_record'][field_name] = validation['sanitized_value']
            
            if not validation['is_valid']:
                result['is_valid'] = False
                result['field_errors'][field_name] = validation['errors']
        
        return result
    
    def validate_batch(self, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Valida un batch di record.
        
        Returns:
            Dict con statistiche di validazione e record sanitizzati
        """
        result = {
            'total_records': len(batch),
            'valid_records': 0,
            'invalid_records': 0,
            'sanitized_batch': [],
            'validation_errors': []
        }
        
        for i, record in enumerate(batch):
            validation = self.validate_record(record)
            
            if validation['is_valid']:
                result['valid_records'] += 1
                result['sanitized_batch'].append(validation['sanitized_record'])
            else:
                result['invalid_records'] += 1
                result['validation_errors'].append({
                    'record_index': i,
                    'errors': validation['field_errors']
                })
                # Aggiungi comunque il record sanitizzato
                result['sanitized_batch'].append(validation['sanitized_record'])
        
        return result