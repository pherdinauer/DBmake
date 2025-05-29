"""
DatabaseManager - Context Manager centralizzato per operazioni MySQL.
"""

import os
import time
import logging
import mysql.connector
from typing import Dict, List, Tuple, Any, Optional, Union, Set, DefaultDict
from dotenv import load_dotenv

# Carica le variabili d'ambiente
load_dotenv()

# Parametri di connessione da variabili d'ambiente
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_USER = os.environ.get('MYSQL_USER', 'Nando')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'DataBase2025!')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'anac_import3')

# Logger del modulo
db_logger = logging.getLogger(__name__)

def log_error_with_context(logger_instance: logging.Logger, error: Exception, context: str = "", operation: str = "") -> None:
    """Helper per logging errori con contesto."""
    context_str = f"[{context}] " if context else ""
    operation_str = f" durante {operation}" if operation else ""
    logger_instance.error(f"❌ {context_str}Errore{operation_str}: {error}")


class DatabaseManager:
    """
    Context Manager centralizzato per tutte le operazioni di database.
    Gestisce connessioni singole, pool di connessioni e configurazione MySQL.
    """
    
    _pool: Optional[Any] = None
    _pool_config: Optional[Dict[str, Any]] = None
    _initialized: bool = False
    
    def __init__(self, use_pool: bool = False, pool_size: int = 2) -> None:
        self.use_pool = use_pool
        self.pool_size = pool_size
        self.connection: Optional[Any] = None
        
        # Configurazione MySQL standard
        self.config: Dict[str, Any] = {
            'host': MYSQL_HOST,
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD,
            'database': MYSQL_DATABASE,
            'charset': 'utf8mb4',
            'autocommit': True,
            'connect_timeout': 180,
            'use_pure': True,
            'ssl_disabled': True,
            'get_warnings': True,
            'raise_on_warnings': True,
            'consume_results': True,
            'buffered': True,
            'raw': False,
            'use_unicode': True,
            'auth_plugin': 'mysql_native_password'
        }
    
    @classmethod
    def initialize_pool(cls, pool_size: int = 2) -> Any:
        """Inizializza il pool di connessioni globale."""
        if cls._pool is not None:
            db_logger.warning("Pool già inizializzato, skip...")
            return cls._pool
            
        try:
            cls._pool_config = {
                'pool_name': "anac_import_pool",
                'pool_size': pool_size,
                'host': MYSQL_HOST,
                'user': MYSQL_USER,
                'password': MYSQL_PASSWORD,
                'database': MYSQL_DATABASE,
                'charset': 'utf8mb4',
                'autocommit': True,
                'connect_timeout': 180,
                'use_pure': True,
                'ssl_disabled': True,
                'get_warnings': True,
                'raise_on_warnings': True,
                'consume_results': True,
                'buffered': True,
                'raw': False,
                'use_unicode': True,
                'auth_plugin': 'mysql_native_password'
            }
            
            # Crea il database se non esiste
            cls._ensure_database_exists()
            
            cls._pool = mysql.connector.pooling.MySQLConnectionPool(**cls._pool_config)
            cls._initialized = True
            
            db_logger.info(f"✅ Pool MySQL inizializzato: {pool_size} connessioni")
            return cls._pool
            
        except Exception as e:
            log_error_with_context(db_logger, e, "pool initialization")
            raise
    
    @classmethod
    def _ensure_database_exists(cls) -> None:
        """Assicura che il database esista, creandolo se necessario."""
        temp_config = cls._pool_config.copy() if cls._pool_config else {
            'host': MYSQL_HOST,
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD,
            'charset': 'utf8mb4',
            'autocommit': True,
            'ssl_disabled': True
        }
        
        # Rimuovi il database dal config per la connessione iniziale
        temp_config.pop('database', None)
        temp_config.pop('pool_name', None)
        temp_config.pop('pool_size', None)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                db_logger.info(f"🔍 Verifica database {MYSQL_DATABASE} (tentativo {attempt + 1}/{max_retries})")
                
                temp_conn = mysql.connector.connect(**temp_config)
                cursor = temp_conn.cursor()
                
                # Controlla se il database esiste
                cursor.execute("SHOW DATABASES LIKE %s", (MYSQL_DATABASE,))
                if not cursor.fetchone():
                    db_logger.info(f"🆕 Creazione database {MYSQL_DATABASE}...")
                    cursor.execute(f"CREATE DATABASE {MYSQL_DATABASE} DEFAULT CHARACTER SET 'utf8mb4'")
                    db_logger.info(f"✅ Database {MYSQL_DATABASE} creato")
                else:
                    db_logger.info(f"✅ Database {MYSQL_DATABASE} già esistente")
                
                cursor.close()
                temp_conn.close()
                return
                
            except mysql.connector.Error as e:
                if attempt < max_retries - 1:
                    db_logger.warning(f"⚠️ Tentativo {attempt + 1} fallito: {e}")
                    time.sleep(2)
                else:
                    log_error_with_context(db_logger, e, "database creation")
                    raise
    
    @classmethod
    def get_pool_connection(cls) -> Any:
        """Ottiene una connessione dal pool."""
        if not cls._initialized:
            cls.initialize_pool()
        return cls._pool.get_connection()
    
    @classmethod
    def close_pool(cls) -> None:
        """Chiude il pool di connessioni."""
        if cls._pool:
            # Note: mysql.connector pools don't have explicit close method
            cls._pool = None
            cls._initialized = False
            db_logger.info("🔒 Pool MySQL chiuso")
    
    def _create_single_connection(self) -> Any:
        """Crea una singola connessione con retry."""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                db_logger.info(f"🔄 Connessione MySQL (tentativo {attempt + 1}/{max_retries})")
                
                # Assicura che il database esista
                self._ensure_database_exists()
                
                conn = mysql.connector.connect(**self.config)
                
                # Configura parametri MySQL per performance
                cursor = conn.cursor()
                cursor.execute("SET GLOBAL max_allowed_packet=1073741824")  # 1GB
                cursor.execute("SET GLOBAL net_write_timeout=600")  # 10 minuti
                cursor.execute("SET GLOBAL net_read_timeout=600")   # 10 minuti
                cursor.execute("SET GLOBAL wait_timeout=600")       # 10 minuti
                cursor.execute("SET GLOBAL interactive_timeout=600") # 10 minuti
                cursor.close()
                
                db_logger.info("✅ Connessione MySQL stabilita")
                return conn
                
            except mysql.connector.Error as e:
                if attempt < max_retries - 1:
                    db_logger.warning(f"⚠️ Tentativo {attempt + 1} fallito: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    log_error_with_context(db_logger, e, "single connection", f"fallito dopo {max_retries} tentativi")
                    raise
    
    def __enter__(self) -> Any:
        """Context manager entry."""
        if self.use_pool:
            self.connection = self.get_pool_connection()
        else:
            self.connection = self._create_single_connection()
        return self.connection
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        """Context manager exit."""
        if self.connection:
            try:
                if exc_type is None:
                    self.connection.commit()
                else:
                    self.connection.rollback()
                    db_logger.warning(f"Rollback per errore: {exc_val}")
            except Exception as e:
                log_error_with_context(db_logger, e, "transaction finalization")
            finally:
                self.connection.close()
                self.connection = None
    
    @staticmethod
    def get_connection() -> 'DatabaseManager':
        """Factory method per connessione singola."""
        return DatabaseManager(use_pool=False)
    
    @staticmethod
    def get_pooled_connection() -> 'DatabaseManager':
        """Factory method per connessione dal pool."""
        return DatabaseManager(use_pool=True) 