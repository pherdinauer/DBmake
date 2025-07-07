"""
Sistema di logging sicuro per ANAC Importer.
Filtra automaticamente credenziali e dati sensibili dai log.
"""

import logging
import re
from typing import List, Pattern, Dict, Any
import json

class SecurityFilter(logging.Filter):
    """
    Filtro di sicurezza per rimuovere informazioni sensibili dai log.
    
    Filtra automaticamente:
    - Password e credenziali
    - Token di autenticazione
    - Dati personali (codici fiscali, email)
    - Query SQL con parametri sensibili
    """
    
    def __init__(self):
        super().__init__()
        self.sensitive_patterns = self._compile_sensitive_patterns()
        self.replacement_text = "[FILTERED]"
    
    def _compile_sensitive_patterns(self) -> List[Pattern]:
        """Compila i pattern regex per identificare dati sensibili."""
        patterns = [
            # Password e credenziali
            re.compile(r'password["\s]*[:=]["\s]*[^"\s,}]+', re.IGNORECASE),
            re.compile(r'passwd["\s]*[:=]["\s]*[^"\s,}]+', re.IGNORECASE),
            re.compile(r'pwd["\s]*[:=]["\s]*[^"\s,}]+', re.IGNORECASE),
            re.compile(r'MYSQL_PASSWORD["\s]*[:=]["\s]*[^"\s,}]+', re.IGNORECASE),
            
            # Token e chiavi API
            re.compile(r'token["\s]*[:=]["\s]*[^"\s,}]+', re.IGNORECASE),
            re.compile(r'api[_-]?key["\s]*[:=]["\s]*[^"\s,}]+', re.IGNORECASE),
            re.compile(r'secret["\s]*[:=]["\s]*[^"\s,}]+', re.IGNORECASE),
            
            # Codici fiscali italiani
            re.compile(r'\b[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]\b'),
            
            # Email addresses
            re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            
            # Numeri di telefono italiani
            re.compile(r'\b(?:\+39\s?)?(?:0\d{1,4}\s?)?\d{6,10}\b'),
            
            # Partite IVA italiane
            re.compile(r'\b\d{11}\b'),
            
            # Pattern per query SQL con valori sensibili
            re.compile(r"VALUES\s*\([^)]*'[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]'[^)]*\)", re.IGNORECASE),
        ]
        return patterns
    
    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filtra il record di log rimuovendo informazioni sensibili.
        
        Args:
            record: Record di log da filtrare
            
        Returns:
            True se il record deve essere loggato, False altrimenti
        """
        try:
            # Filtra il messaggio principale
            if hasattr(record, 'msg') and record.msg:
                record.msg = self._filter_sensitive_data(str(record.msg))
            
            # Filtra gli argomenti
            if hasattr(record, 'args') and record.args:
                filtered_args = []
                for arg in record.args:
                    if isinstance(arg, str):
                        filtered_args.append(self._filter_sensitive_data(arg))
                    else:
                        filtered_args.append(arg)
                record.args = tuple(filtered_args)
            
            return True
            
        except Exception as e:
            # In caso di errore nel filtering, logga l'errore ma non bloccare
            print(f"Errore nel filtro di sicurezza: {e}")
            return True
    
    def _filter_sensitive_data(self, text: str) -> str:
        """
        Filtra i dati sensibili da una stringa di testo.
        
        Args:
            text: Testo da filtrare
            
        Returns:
            Testo con dati sensibili rimossi
        """
        filtered_text = text
        
        for pattern in self.sensitive_patterns:
            filtered_text = pattern.sub(self.replacement_text, filtered_text)
        
        return filtered_text

class SecureLogger:
    """
    Logger sicuro che integra automaticamente il filtro di sicurezza.
    
    Caratteristiche:
    - Filtro automatico di dati sensibili
    - Rotazione dei log
    - Livelli di log configurabili
    - Audit trail per operazioni sensibili
    """
    
    def __init__(self, name: str, log_file: str = None, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Rimuovi handler esistenti per evitare duplicati
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Configura formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Handler per console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.addFilter(SecurityFilter())
        self.logger.addHandler(console_handler)
        
        # Handler per file (se specificato)
        if log_file:
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_file, 
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
            file_handler.setFormatter(formatter)
            file_handler.addFilter(SecurityFilter())
            self.logger.addHandler(file_handler)
    
    def get_logger(self) -> logging.Logger:
        """Restituisce il logger configurato."""
        return self.logger
    
    def log_security_event(self, event_type: str, details: Dict[str, Any]) -> None:
        """
        Log di eventi di sicurezza con dettagli strutturati.
        
        Args:
            event_type: Tipo di evento (es. 'credential_access', 'auth_failure')
            details: Dettagli dell'evento (verranno filtrati automaticamente)
        """
        # Filtra i dettagli prima del logging
        filtered_details = self._filter_dict_values(details)
        
        security_msg = f"SECURITY_EVENT: {event_type} | {json.dumps(filtered_details)}"
        self.logger.warning(security_msg)
    
    def _filter_dict_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Filtra i valori sensibili da un dizionario."""
        filtered = {}
        security_filter = SecurityFilter()
        
        for key, value in data.items():
            if isinstance(value, str):
                filtered[key] = security_filter._filter_sensitive_data(value)
            elif key.lower() in ['password', 'passwd', 'pwd', 'token', 'secret']:
                filtered[key] = "[FILTERED]"
            else:
                filtered[key] = value
        
        return filtered

# Factory function per creare logger sicuri
def create_secure_logger(name: str, log_file: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Crea un logger sicuro con filtro automatico.
    
    Args:
        name: Nome del logger
        log_file: File di log (opzionale)
        level: Livello di logging
        
    Returns:
        Logger configurato con filtro di sicurezza
    """
    secure_logger = SecureLogger(name, log_file, level)
    return secure_logger.get_logger()