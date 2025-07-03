"""
Gestore sicuro delle connessioni database con SSL, pooling e retry logic.
"""

import mysql.connector
from mysql.connector import pooling, Error
from typing import Optional, Dict, Any, ContextManager
import logging
import time
import ssl
from contextlib import contextmanager
import threading

logger = logging.getLogger(__name__)

class SecureDatabaseConnection:
    """
    Gestore sicuro delle connessioni database con caratteristiche enterprise:
    
    - SSL obbligatorio per sicurezza
    - Connection pooling per performance
    - Retry automatico per resilienza
    - Timeout configurabili
    - Monitoring delle connessioni
    """
    
    def __init__(self, credentials: Dict[str, Any], pool_size: int = 5):
        self.credentials = credentials
        self.pool_size = pool_size
        self.connection_pool = None
        self._pool_lock = threading.Lock()
        self._setup_connection_pool()
    
    def _setup_connection_pool(self) -> None:
        """Configura il pool di connessioni con impostazioni sicure."""
        try:
            # Configurazione SSL sicura
            ssl_config = self._get_ssl_config()
            
            # Configurazione pool
            pool_config = {
                'pool_name': 'anac_importer_pool',
                'pool_size': self.pool_size,
                'pool_reset_session': True,
                'host': self.credentials['host'],
                'user': self.credentials['user'],
                'password': self.credentials['password'],
                'database': self.credentials['database'],
                'port': self.credentials.get('port', 3306),
                'autocommit': False,  # Transazioni esplicite per sicurezza
                'connect_timeout': 30,
                'sql_mode': 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION',
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_unicode_ci',
                'raise_on_warnings': True,
            }
            
            # Aggiungi SSL se non disabilitato
            if not self.credentials.get('ssl_disabled', False):
                pool_config.update(ssl_config)
                logger.info("SSL abilitato per connessioni database")
            else:
                logger.warning("⚠️ SSL DISABILITATO - connessione non sicura!")
            
            self.connection_pool = pooling.MySQLConnectionPool(**pool_config)
            logger.info(f"Pool di connessioni creato: {self.pool_size} connessioni")
            
        except Error as e:
            logger.error(f"Errore nella creazione del pool di connessioni: {e}")
            raise
    
    def _get_ssl_config(self) -> Dict[str, Any]:
        """Configura SSL con impostazioni sicure."""
        return {
            'ssl_disabled': False,
            'ssl_verify_cert': True,
            'ssl_verify_identity': False,  # Può essere True in produzione con certificati validi
            'ssl_ca': None,  # Path al certificato CA se necessario
            'ssl_cert': None,  # Path al certificato client se necessario
            'ssl_key': None,  # Path alla chiave privata se necessario
        }
    
    @contextmanager
    def get_connection(self, autocommit: bool = False):
        """
        Context manager per ottenere una connessione dal pool.
        
        Args:
            autocommit: Se True, abilita autocommit
            
        Yields:
            Connessione database
        """
        connection = None
        try:
            with self._pool_lock:
                connection = self.connection_pool.get_connection()
                
            if connection:
                connection.autocommit = autocommit
                logger.debug("Connessione ottenuta dal pool")
                yield connection
            else:
                raise Exception("Impossibile ottenere connessione dal pool")
                
        except Error as e:
            logger.error(f"Errore nella connessione database: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
                logger.debug("Connessione restituita al pool")
    
    def execute_with_retry(self, query: str, params: tuple = None, max_retries: int = 3, autocommit: bool = False):
        """
        Esegue una query con retry automatico.
        
        Args:
            query: Query SQL da eseguire
            params: Parametri per la query
            max_retries: Numero massimo di tentativi
            autocommit: Se True, abilita autocommit
            
        Returns:
            Risultato della query
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                with self.get_connection(autocommit=autocommit) as conn:
                    cursor = conn.cursor()
                    try:
                        if params:
                            cursor.execute(query, params)
                        else:
                            cursor.execute(query)
                        
                        # Commit se non in autocommit
                        if not autocommit:
                            conn.commit()
                        
                        # Recupera risultati se è una SELECT
                        if query.strip().upper().startswith('SELECT'):
                            return cursor.fetchall()
                        else:
                            return cursor.rowcount
                            
                    finally:
                        cursor.close()
                        
            except Error as e:
                last_error = e
                logger.warning(f"Tentativo {attempt + 1}/{max_retries} fallito: {e}")
                
                # Aspetta prima di riprovare
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff esponenziale
        
        # Se tutti i tentativi falliscono
        logger.error(f"Query fallita dopo {max_retries} tentativi: {last_error}")
        raise last_error
    
    def execute_transaction(self, operations: list, max_retries: int = 3):
        """
        Esegue multiple operazioni in una transazione ACID.
        
        Args:
            operations: Lista di tuple (query, params)
            max_retries: Numero massimo di tentativi
            
        Returns:
            True se successo, False altrimenti
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                with self.get_connection(autocommit=False) as conn:
                    cursor = conn.cursor()
                    try:
                        # Esegui tutte le operazioni
                        for query, params in operations:
                            if params:
                                cursor.execute(query, params)
                            else:
                                cursor.execute(query)
                        
                        # Commit della transazione
                        conn.commit()
                        logger.info(f"Transazione completata: {len(operations)} operazioni")
                        return True
                        
                    except Exception as e:
                        # Rollback in caso di errore
                        conn.rollback()
                        logger.error(f"Errore in transazione, rollback eseguito: {e}")
                        raise
                    finally:
                        cursor.close()
                        
            except Error as e:
                last_error = e
                logger.warning(f"Transazione tentativo {attempt + 1}/{max_retries} fallito: {e}")
                
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"Transazione fallita dopo {max_retries} tentativi: {last_error}")
        return False
    
    def test_connection(self) -> bool:
        """Testa la connettività del database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                
                if result and result[0] == 1:
                    logger.info("Test connessione database: OK")
                    return True
                else:
                    logger.error("Test connessione database: FALLITO")
                    return False
                    
        except Exception as e:
            logger.error(f"Test connessione database fallito: {e}")
            return False
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Restituisce statistiche del pool di connessioni."""
        if not self.connection_pool:
            return {}
        
        return {
            'pool_name': self.connection_pool.pool_name,
            'pool_size': self.connection_pool.pool_size,
            'pool_reset_session': self.connection_pool.pool_reset_session,
        }
    
    def close_pool(self) -> None:
        """Chiude il pool di connessioni."""
        if self.connection_pool:
            # Non c'è un metodo diretto per chiudere il pool in mysql-connector-python
            # Le connessioni si chiudono automaticamente quando escono dallo scope
            logger.info("Pool di connessioni chiuso")
            self.connection_pool = None