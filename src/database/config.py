"""
Database configuration module.
Centralizza la configurazione del database e permette modifiche dinamiche.
"""

import os
from typing import Dict, Any

class DatabaseConfig:
    """Gestisce la configurazione dinamica del database."""
    
    _config: Dict[str, Any] = None
    _initialized: bool = False
    
    @classmethod
    def initialize(cls, custom_database_name: str = None) -> Dict[str, Any]:
        """Inizializza la configurazione del database."""
        if cls._config is None or custom_database_name:
            cls._config = {
                'host': os.environ.get('MYSQL_HOST', 'localhost'),
                'user': os.environ.get('MYSQL_USER', 'Nando'),
                'password': os.environ.get('MYSQL_PASSWORD', 'DataBase2025!'),
                'database': custom_database_name or os.environ.get('MYSQL_DATABASE', 'anac_import3'),
                'charset': 'utf8mb4',
                'autocommit': True,
                'connect_timeout': 300,
                'use_pure': True,
                'ssl_disabled': True,
                'get_warnings': False,
                'raise_on_warnings': False,
                'consume_results': True,
                'buffered': True,
                'raw': False,
                'use_unicode': True,
                'auth_plugin': 'mysql_native_password',
                'connection_timeout': 300,
                'sql_mode': '',
            }
            
            # Aggiorna anche la variabile d'ambiente per consistenza
            if custom_database_name:
                os.environ['MYSQL_DATABASE'] = custom_database_name
            
            cls._initialized = True
            
        return cls._config.copy()
    
    @classmethod
    def get_config(cls) -> Dict[str, Any]:
        """Restituisce la configurazione corrente del database."""
        if not cls._initialized:
            return cls.initialize()
        return cls._config.copy()
    
    @classmethod
    def get_database_name(cls) -> str:
        """Restituisce il nome del database corrente."""
        config = cls.get_config()
        return config['database']
    
    @classmethod
    def set_database_name(cls, database_name: str) -> None:
        """Cambia il nome del database e reinizializza la configurazione."""
        cls._config = None
        cls._initialized = False
        cls.initialize(database_name)
    
    @classmethod
    def reset(cls) -> None:
        """Reset della configurazione."""
        cls._config = None
        cls._initialized = False

# Mantieni compatibilità con il codice esistente
def get_mysql_config(custom_database_name: str = None) -> Dict[str, Any]:
    """Funzione di compatibilità per ottenere la configurazione MySQL."""
    return DatabaseConfig.initialize(custom_database_name) 