"""
DatabaseManager - Context Manager centralizzato per operazioni MySQL.
"""

import os
import time
import logging
import mysql.connector
from mysql.connector import pooling, errorcode
from typing import Dict, List, Tuple, Any, Optional, Union, Set, DefaultDict
from dotenv import load_dotenv
from contextlib import contextmanager

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
    
    actual_error_type_name = type(error).__name__
    # Robust check for InterfaceError
    is_interface_error = (
        actual_error_type_name == 'MySQLInterfaceError' or
        actual_error_type_name == 'InterfaceError' or
        isinstance(error, mysql.connector.errors.InterfaceError)
    )

    # Debug log to see what type is being processed
    logger_instance.info(f"DEBUG log_error_with_context: Processing error of type '{actual_error_type_name}', detected as InterfaceError: {is_interface_error}")

    if is_interface_error:
        errno = getattr(error, 'errno', 'N/A')
        sqlstate = getattr(error, 'sqlstate', 'N/A')
        safe_error_message = (
            f"A MySQL Interface Error occurred (type: {actual_error_type_name}, errno: {errno}, sqlstate: {sqlstate}). "
            f"Operation failed. Original message details suppressed due to formatting issues."
        )
        logger_instance.error(f"[ERROR] {context_str}Errore{operation_str}: {safe_error_message}")
    else:
        logger_instance.error(f"[ERROR] {context_str}Errore{operation_str}: {error}")


class DatabaseManager:
    """
    Context Manager centralizzato per tutte le operazioni di database.
    Gestisce connessioni singole, pool di connessioni e configurazione MySQL con riconnessione automatica.
    """
    
    _pool: Optional[Any] = None
    _pool_config: Optional[Dict[str, Any]] = None
    _initialized: bool = False
    
    def __init__(self, use_pool: bool = False, pool_size: int = 2) -> None:
        self.use_pool = use_pool
        self.pool_size = pool_size
        self.connection: Optional[Any] = None
        self.max_reconnect_attempts = 3
        self.reconnect_delay = 2  # secondi
        
        # Configurazione MySQL ottimizzata per operazioni lunghe
        self.config: Dict[str, Any] = {
            'host': MYSQL_HOST,
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD,
            'database': MYSQL_DATABASE,
            'charset': 'utf8mb4',
            'autocommit': True,
            'connect_timeout': 300,  # Aumentato a 5 minuti
            'use_pure': True,
            'ssl_disabled': True,
            'get_warnings': False,  # Disabilitato per evitare problemi di configurazione
            'raise_on_warnings': False,  # Disabilitato per evitare problemi di configurazione
            'consume_results': True,
            'buffered': True,
            'raw': False,
            'use_unicode': True,
            'auth_plugin': 'mysql_native_password',
            # Aggiunte per stabilità connessione
            'connection_timeout': 300,  # 5 minuti
            'autocommit': True,
            'sql_mode': '',  # Mode SQL più permissivo
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
            
            db_logger.info(f"[OK] Pool MySQL inizializzato: {pool_size} connessioni")
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
            'ssl_disabled': True,
            'connect_timeout': 30,
            'use_pure': True
        }
        
        # Rimuovi il database dal config per la connessione iniziale
        temp_config.pop('database', None)
        temp_config.pop('pool_name', None)
        temp_config.pop('pool_size', None)
        
        max_retries = 3
        for attempt in range(max_retries):
            temp_conn = None
            cursor = None
            try:
                db_logger.info(f"[CHECK] Verifica database {MYSQL_DATABASE} (tentativo {attempt + 1}/{max_retries})")
                
                temp_conn = mysql.connector.connect(**temp_config)
                cursor = temp_conn.cursor()
                
                # Controlla se il database esiste
                cursor.execute("SHOW DATABASES LIKE %s", (MYSQL_DATABASE,))
                if not cursor.fetchone():
                    db_logger.info(f"[CREATE] Creazione database {MYSQL_DATABASE}...")
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE} DEFAULT CHARACTER SET 'utf8mb4'")
                    db_logger.info(f"[OK] Database {MYSQL_DATABASE} creato con successo")
                else:
                    db_logger.info(f"[OK] Database {MYSQL_DATABASE} già esistente")
                
                return  # Successo, esci dalla funzione
                
            except mysql.connector.Error as e:
                actual_error_type_name = type(e).__name__
                errno = getattr(e, 'errno', 'N/A')
                sqlstate = getattr(e, 'sqlstate', 'N/A')
                db_logger.info(f"DEBUG _ensure_database_exists: Caught mysql.connector.Error. Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}")

                is_interface_error = (
                    actual_error_type_name == 'MySQLInterfaceError' or
                    actual_error_type_name == 'InterfaceError' or
                    isinstance(e, mysql.connector.errors.InterfaceError)
                )

                if is_interface_error:
                    error_msg_content = f"A MySQL Interface Error occurred (type: {actual_error_type_name}, errno: {errno}, sqlstate: {sqlstate}). Operation failed."
                else:
                    if e.args and len(e.args) > 0 and isinstance(e.args[0], str):
                        error_msg_content = e.args[0]
                    elif e.args and len(e.args) > 1 and isinstance(e.args[1], str):
                        error_msg_content = e.args[1]
                    else:
                        try:
                            error_msg_content = str(e)
                        except Exception as str_conv_ex:
                            error_msg_content = f"Failed to convert MySQL error to string: {str_conv_ex}"
                
                error_msg = f"MySQL Error (Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}): {error_msg_content}"

                if "object has no attribute 'msg'" in error_msg: # Final safeguard
                    error_msg = f"MySQL Error (Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}): Problematic error string detected and suppressed."

                if attempt < max_retries - 1:
                    db_logger.warning(f"[WARN] Tentativo {attempt + 1} fallito: {error_msg}")
                    time.sleep(2)
                else:
                    db_logger.error(f"[ERROR] Errore durante la creazione del database: {error_msg}")
                    raise
            except Exception as e: # Generic catch-all for _ensure_database_exists
                actual_error_type_name = type(e).__name__
                db_logger.info(f"DEBUG _ensure_database_exists: Caught generic Exception. Type: {actual_error_type_name}")
                is_interface_error_generic = (
                    actual_error_type_name == 'MySQLInterfaceError' or
                    actual_error_type_name == 'InterfaceError' or
                    isinstance(e, mysql.connector.errors.InterfaceError)
                )

                if is_interface_error_generic:
                    errno = getattr(e, 'errno', 'N/A')
                    sqlstate = getattr(e, 'sqlstate', 'N/A')
                    error_msg = f"MySQL Interface Error (Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}): An Interface Error occurred. Details suppressed."
                elif isinstance(e, mysql.connector.Error):
                    errno = getattr(e, 'errno', 'N/A')
                    sqlstate = getattr(e, 'sqlstate', 'N/A')
                    error_msg = f"Other MySQL Error (Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}): {str(e)}"
                else:
                    error_msg = f"Errore generico: {str(e)}"

                if "object has no attribute 'msg'" in error_msg: # Final safeguard
                    error_msg = f"Generic Exception (Type: {actual_error_type_name}): Problematic error string detected and suppressed."

                if attempt < max_retries - 1:
                    db_logger.warning(f"[WARN] Tentativo {attempt + 1} fallito: {error_msg}")
                    time.sleep(2)
                else:
                    db_logger.error(f"[ERROR] Errore imprevisto durante la connessione: {error_msg}")
                    raise
            finally:
                # Pulisci sempre le risorse
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass
                if temp_conn:
                    try:
                        temp_conn.close()
                    except:
                        pass
    
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
            db_logger.info("[CLOSE] Pool MySQL chiuso")
    
    def _create_single_connection(self) -> Any:
        """Crea una singola connessione con retry."""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            conn = None
            cursor = None
            try:
                db_logger.info(f"[CONNECT] Connessione MySQL (tentativo {attempt + 1}/{max_retries})")
                
                # Assicura che il database esista
                self._ensure_database_exists()
                
                conn = mysql.connector.connect(**self.config)
                
                # Configura parametri MySQL per performance
                cursor = conn.cursor()
                try:
                    cursor.execute("SET SESSION max_allowed_packet=1073741824")  # 1GB - usa SESSION invece di GLOBAL
                    cursor.execute("SET SESSION net_write_timeout=600")  # 10 minuti
                    cursor.execute("SET SESSION net_read_timeout=600")   # 10 minuti
                    cursor.execute("SET SESSION wait_timeout=600")       # 10 minuti
                    cursor.execute("SET SESSION interactive_timeout=600") # 10 minuti
                except mysql.connector.Error as config_error:
                    # Se non riusciamo a configurare, logga ma continua
                    db_logger.warning(f"[WARN] Configurazione sessione MySQL fallita: {config_error}")
                finally:
                    if cursor:
                        cursor.close()
                
                db_logger.info("[OK] Connessione MySQL stabilita")
                return conn
                
            except mysql.connector.Error as e:
                actual_error_type_name = type(e).__name__
                errno = getattr(e, 'errno', 'N/A')
                sqlstate = getattr(e, 'sqlstate', 'N/A')
                db_logger.info(f"DEBUG _create_single_connection: Caught mysql.connector.Error. Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}")

                is_interface_error = (
                    actual_error_type_name == 'MySQLInterfaceError' or
                    actual_error_type_name == 'InterfaceError' or
                    isinstance(e, mysql.connector.errors.InterfaceError)
                )

                if is_interface_error:
                    error_msg_content = f"A MySQL Interface Error occurred (type: {actual_error_type_name}, errno: {errno}, sqlstate: {sqlstate}). Operation failed."
                else:
                    if e.args and len(e.args) > 0 and isinstance(e.args[0], str):
                        error_msg_content = e.args[0]
                    elif e.args and len(e.args) > 1 and isinstance(e.args[1], str):
                        error_msg_content = e.args[1]
                    else:
                        try:
                            error_msg_content = str(e)
                        except Exception as str_conv_ex:
                            error_msg_content = f"Failed to convert MySQL error to string: {str_conv_ex}"
                
                error_msg = f"MySQL Error (Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}): {error_msg_content}"

                if "object has no attribute 'msg'" in error_msg: # Final safeguard
                    error_msg = f"MySQL Error (Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}): Problematic error string detected and suppressed."

                if attempt < max_retries - 1:
                    db_logger.warning(f"[WARN] Tentativo {attempt + 1} fallito: {error_msg}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    db_logger.error(f"[ERROR] Connessione fallita dopo {max_retries} tentativi: {error_msg}")
                    raise
            except Exception as e: # Generic catch-all for _create_single_connection
                actual_error_type_name = type(e).__name__
                db_logger.info(f"DEBUG _create_single_connection: Caught generic Exception. Type: {actual_error_type_name}")
                is_interface_error_generic = (
                    actual_error_type_name == 'MySQLInterfaceError' or
                    actual_error_type_name == 'InterfaceError' or
                    isinstance(e, mysql.connector.errors.InterfaceError)
                )

                if is_interface_error_generic:
                    errno = getattr(e, 'errno', 'N/A')
                    sqlstate = getattr(e, 'sqlstate', 'N/A')
                    error_msg = f"MySQL Interface Error (Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}): An Interface Error occurred. Details suppressed."
                elif isinstance(e, mysql.connector.Error):
                    errno = getattr(e, 'errno', 'N/A')
                    sqlstate = getattr(e, 'sqlstate', 'N/A')
                    error_msg = f"Other MySQL Error (Type: {actual_error_type_name}, Errno: {errno}, SQLSTATE: {sqlstate}): {str(e)}"
                else:
                    error_msg = f"Errore generico: {str(e)}"
                
                if "object has no attribute 'msg'" in error_msg: # Final safeguard
                    error_msg = f"Generic Exception (Type: {actual_error_type_name}): Problematic error string detected and suppressed."

                if attempt < max_retries - 1:
                    db_logger.warning(f"[WARN] Tentativo {attempt + 1} fallito: {error_msg}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    db_logger.error(f"[ERROR] Errore imprevisto durante la connessione: {error_msg}")
                    raise
            finally:
                # Pulisci in caso di errore
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass
                if conn and attempt < max_retries - 1:  # Solo se non è l'ultima iterazione
                    try:
                        conn.close()
                    except:
                        pass
    
    def _test_connection(self) -> bool:
        """Testa se la connessione è ancora valida."""
        if not self.connection:
            db_logger.info("[TEST_CONNECTION] Connessione non presente")
            return False
        
        try:
            # Test più robusto della connessione
            if hasattr(self.connection, 'is_connected'):
                is_conn_status = self.connection.is_connected()
                db_logger.info(f"[TEST_CONNECTION] is_connected() = {is_conn_status}")
                if not is_conn_status:
                    return False
            
            # Test con query semplice
            db_logger.info("[TEST_CONNECTION] Creazione cursor per test...")
            cursor = self.connection.cursor()
            
            db_logger.info("[TEST_CONNECTION] Esecuzione SELECT 1...")
            cursor.execute("SELECT 1")
            
            db_logger.info("[TEST_CONNECTION] Fetch del risultato...")
            result = cursor.fetchone()
            
            db_logger.info(f"[TEST_CONNECTION] Risultato query: {result}")
            cursor.close()
            
            # Verifica che il risultato sia corretto
            is_valid = result is not None and result[0] == 1
            db_logger.info(f"[TEST_CONNECTION] Test completato: {is_valid}")
            return is_valid
            
        except Exception as e:
            db_logger.info(f"[TEST_CONNECTION] Test fallito con eccezione: {type(e).__name__}: {e}")
            return False
    
    def _reconnect_if_needed(self) -> bool:
        """Riconnette automaticamente se la connessione è persa."""
        if self._test_connection():
            return True
        
        db_logger.warning("[RECONNECT] Connessione persa, tentativo di riconnessione...")
        
        for attempt in range(self.max_reconnect_attempts):
            try:
                # Chiudi la connessione esistente se presente
                if self.connection:
                    try:
                        self.connection.close()
                    except:
                        pass
                    self.connection = None
                
                # Pausa prima di riconnettere
                if attempt > 0:
                    time.sleep(self.reconnect_delay)
                
                # Crea nuova connessione
                self.connection = self._create_single_connection()
                
                # Test immediato della nuova connessione
                if self._test_connection():
                    db_logger.info(f"[RECONNECT] Riconnessione riuscita al tentativo {attempt + 1}")
                    return True
                else:
                    db_logger.warning(f"[RECONNECT] Connessione creata ma test fallito (tentativo {attempt + 1})")
                    
            except Exception as e:
                db_logger.warning(f"[RECONNECT] Tentativo {attempt + 1} fallito: {e}")
                
        db_logger.error("[RECONNECT] Impossibile ristabilire la connessione dopo tutti i tentativi")
        return False
    
    def __enter__(self) -> Any:
        """Context manager entry con riconnessione automatica e test robusto."""
        try:
            if self.use_pool:
                if not self._pool:
                    self.initialize_pool(self.pool_size)
                self.connection = self.get_pool_connection()
                db_logger.info("[ENTER] Connessione dal pool stabilita")
            else:
                self.connection = self._create_single_connection()
                db_logger.info("[ENTER] Connessione diretta stabilita")
                
            # Test della connessione con retry integrato
            max_test_attempts = 3
            for test_attempt in range(max_test_attempts):
                if self._test_connection():
                    db_logger.debug(f"[ENTER] Test connessione riuscito (tentativo {test_attempt + 1})")
                    return self.connection
                else:
                    db_logger.warning(f"[ENTER] Test connessione fallito (tentativo {test_attempt + 1}/{max_test_attempts})")
                    
                    if test_attempt < max_test_attempts - 1:
                        # Riprova a creare la connessione
                        try:
                            if self.connection:
                                self.connection.close()
                        except:
                            pass
                        
                        time.sleep(1)  # Pausa breve
                        self.connection = self._create_single_connection()
                        db_logger.info(f"[ENTER] Ricreo connessione per test (tentativo {test_attempt + 2})")
            
            # Se tutti i test falliscono, solleva un'eccezione più specifica
            raise mysql.connector.Error("Impossibile stabilire una connessione MySQL valida dopo multipli tentativi")
                
        except Exception as e:
            # Log dettagliato dell'errore
            error_msg = f"Errore durante stabilimento connessione: {e}"
            db_logger.error(f"[ENTER] {error_msg}")
            
            # Pulizia in caso di errore
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
                self.connection = None
            
            raise mysql.connector.Error(error_msg)
    
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
    
    def execute_with_retry(self, query: str, params: Optional[tuple] = None, fetch: bool = False) -> Any:
        """Esegue una query con riconnessione automatica in caso di errore."""
        for attempt in range(self.max_reconnect_attempts):
            try:
                if not self._test_connection():
                    if not self._reconnect_if_needed():
                        raise mysql.connector.Error("Impossibile riconnettersi al database")
                
                cursor = self.connection.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                result = None
                if fetch:
                    result = cursor.fetchall()
                
                cursor.close()
                return result
                
            except mysql.connector.Error as e:
                error_type = type(e).__name__
                errno = getattr(e, 'errno', 'N/A')
                
                # Errori di connessione che richiedono retry
                connection_errors = [2003, 2006, 2013, 2055]  # Can't connect, gone away, lost connection, bad file descriptor
                
                if errno in connection_errors or "Lost connection" in str(e) or "Bad file descriptor" in str(e):
                    db_logger.warning(f"[RETRY] Errore di connessione (errno: {errno}), tentativo {attempt + 1}")
                    self.connection = None  # Forza riconnessione
                    if attempt < self.max_reconnect_attempts - 1:
                        continue
                
                # Per altri errori, rilancia immediatamente
                log_error_with_context(db_logger, e, "execute_with_retry", f"query: {query[:100]}...")
                raise
        
        raise mysql.connector.Error("Query fallita dopo tutti i tentativi di riconnessione") 