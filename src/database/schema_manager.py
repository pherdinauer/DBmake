"""
Schema Manager per l'architettura enterprise ANAC Importer.
Gestisce la creazione e l'aggiornamento automatico dello schema database.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class SchemaManager:
    """
    Gestore dello schema database enterprise.
    
    Caratteristiche:
    - Creazione automatica tabelle
    - Migrazione schema
    - Verifica integrità strutturale
    - Supporto per audit trail
    """
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.schema_version = "1.0.0"
    
    def initialize_enterprise_schema(self) -> bool:
        """
        Inizializza lo schema completo per l'architettura enterprise.
        
        Returns:
            True se schema creato/aggiornato con successo
        """
        try:
            logger.info("🏗️ Inizializzazione schema enterprise...")
            
            # Crea tabelle di sistema
            self._create_system_tables()
            
            # Crea tabelle per job management
            self._create_job_management_tables()
            
            # Crea tabelle per dati ANAC
            self._create_anac_data_tables()
            
            # Crea tabelle di audit
            self._create_audit_tables()
            
            # Registra versione schema
            self._register_schema_version()
            
            logger.info("✅ Schema enterprise inizializzato con successo")
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inizializzazione schema: {e}")
            return False
    
    def _create_system_tables(self) -> None:
        """Crea le tabelle di sistema."""
        
        # Tabella per versioni schema
        schema_versions_sql = """
        CREATE TABLE IF NOT EXISTS schema_versions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            version VARCHAR(20) NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT,
            INDEX idx_version (version)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # Tabella per configurazioni di sistema
        system_config_sql = """
        CREATE TABLE IF NOT EXISTS system_config (
            config_key VARCHAR(100) PRIMARY KEY,
            config_value TEXT,
            config_type ENUM('string', 'integer', 'boolean', 'json') DEFAULT 'string',
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        self._execute_sql(schema_versions_sql)
        self._execute_sql(system_config_sql)
        
        logger.info("✅ Tabelle di sistema create")
    
    def _create_job_management_tables(self) -> None:
        """Crea le tabelle per la gestione dei job."""
        
        # Tabella principale dei job
        import_jobs_sql = """
        CREATE TABLE IF NOT EXISTS import_jobs (
            job_id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            status ENUM('pending', 'running', 'completed', 'failed', 'rolled_back') NOT NULL DEFAULT 'pending',
            validation_level ENUM('basic', 'strict', 'enterprise') NOT NULL DEFAULT 'strict',
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP NULL,
            completed_at TIMESTAMP NULL,
            
            total_records INT DEFAULT 0,
            processed_records INT DEFAULT 0,
            valid_records INT DEFAULT 0,
            invalid_records INT DEFAULT 0,
            failed_records INT DEFAULT 0,
            
            source_checksum VARCHAR(64),
            target_checksum VARCHAR(64),
            integrity_verified BOOLEAN DEFAULT FALSE,
            
            created_by VARCHAR(100),
            
            INDEX idx_status (status),
            INDEX idx_created_at (created_at),
            INDEX idx_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # Tabella per i batch
        import_batches_sql = """
        CREATE TABLE IF NOT EXISTS import_batches (
            batch_id VARCHAR(36) PRIMARY KEY,
            job_id VARCHAR(36) NOT NULL,
            status ENUM('pending', 'running', 'completed', 'failed') NOT NULL DEFAULT 'pending',
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP NULL,
            
            record_count INT DEFAULT 0,
            checksum VARCHAR(64),
            
            source_file VARCHAR(500),
            
            FOREIGN KEY (job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE,
            INDEX idx_job_id (job_id),
            INDEX idx_status (status),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # Tabella per messaggi di job (errori, warning, info)
        job_messages_sql = """
        CREATE TABLE IF NOT EXISTS job_messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            job_id VARCHAR(36) NOT NULL,
            batch_id VARCHAR(36) NULL,
            message_type ENUM('error', 'warning', 'info') NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE,
            FOREIGN KEY (batch_id) REFERENCES import_batches(batch_id) ON DELETE SET NULL,
            INDEX idx_job_id (job_id),
            INDEX idx_type (message_type),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        self._execute_sql(import_jobs_sql)
        self._execute_sql(import_batches_sql)
        self._execute_sql(job_messages_sql)
        
        logger.info("✅ Tabelle job management create")
    
    def _create_anac_data_tables(self) -> None:
        """Crea le tabelle per i dati ANAC."""
        
        # Tabella generica per record ANAC
        anac_records_sql = """
        CREATE TABLE IF NOT EXISTS anac_records_generic (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            
            -- Metadati importazione
            _batch_id VARCHAR(36) NOT NULL,
            _import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _checksum VARCHAR(64),
            _source_file VARCHAR(500),
            
            -- Identificatori principali
            cig VARCHAR(10),
            id_aggiudicazione VARCHAR(50),
            
            -- Dati JSON completi
            data_json JSON,
            
            -- Campi estratti comuni
            denominazione_aggiudicatario TEXT,
            codice_fiscale_aggiudicatario VARCHAR(16),
            importo_aggiudicazione DECIMAL(15,2),
            data_aggiudicazione DATE,
            
            -- Indici
            INDEX idx_batch_id (_batch_id),
            INDEX idx_cig (cig),
            INDEX idx_id_aggiudicazione (id_aggiudicazione),
            INDEX idx_import_timestamp (_import_timestamp),
            INDEX idx_checksum (_checksum)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # Tabella specifica per aggiudicazioni
        anac_aggiudicazioni_sql = """
        CREATE TABLE IF NOT EXISTS anac_aggiudicazioni (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            
            -- Metadati importazione
            _batch_id VARCHAR(36) NOT NULL,
            _import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _checksum VARCHAR(64),
            _source_file VARCHAR(500),
            
            -- Dati specifici aggiudicazioni
            cig VARCHAR(10) NOT NULL,
            id_aggiudicazione VARCHAR(50) NOT NULL,
            denominazione_aggiudicatario TEXT,
            codice_fiscale_aggiudicatario VARCHAR(16),
            partita_iva_aggiudicatario VARCHAR(11),
            importo_aggiudicazione DECIMAL(15,2),
            data_aggiudicazione DATE,
            data_stipula DATE,
            
            -- Dati JSON completi per flessibilità
            data_json JSON,
            
            UNIQUE KEY unique_aggiudicazione (cig, id_aggiudicazione),
            INDEX idx_batch_id (_batch_id),
            INDEX idx_cig (cig),
            INDEX idx_cf_aggiudicatario (codice_fiscale_aggiudicatario),
            INDEX idx_data_aggiudicazione (data_aggiudicazione)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # Tabella per partecipanti
        anac_partecipanti_sql = """
        CREATE TABLE IF NOT EXISTS anac_partecipanti (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            
            -- Metadati importazione
            _batch_id VARCHAR(36) NOT NULL,
            _import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _checksum VARCHAR(64),
            _source_file VARCHAR(500),
            
            -- Dati specifici partecipanti
            cig VARCHAR(10) NOT NULL,
            id_partecipante VARCHAR(50) NOT NULL,
            denominazione_partecipante TEXT,
            codice_fiscale_partecipante VARCHAR(16),
            ruolo_partecipante VARCHAR(100),
            
            -- Dati JSON completi
            data_json JSON,
            
            UNIQUE KEY unique_partecipante (cig, id_partecipante),
            INDEX idx_batch_id (_batch_id),
            INDEX idx_cig (cig),
            INDEX idx_cf_partecipante (codice_fiscale_partecipante)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        self._execute_sql(anac_records_sql)
        self._execute_sql(anac_aggiudicazioni_sql)
        self._execute_sql(anac_partecipanti_sql)
        
        logger.info("✅ Tabelle dati ANAC create")
    
    def _create_audit_tables(self) -> None:
        """Crea le tabelle per audit trail."""
        
        # Tabella per audit delle operazioni
        audit_log_sql = """
        CREATE TABLE IF NOT EXISTS audit_log (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            
            -- Identificazione operazione
            operation_type ENUM('INSERT', 'UPDATE', 'DELETE', 'SELECT') NOT NULL,
            table_name VARCHAR(100) NOT NULL,
            record_id VARCHAR(100),
            
            -- Contesto operazione
            job_id VARCHAR(36),
            batch_id VARCHAR(36),
            user_id VARCHAR(100),
            
            -- Dettagli operazione
            operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            old_values JSON,
            new_values JSON,
            
            -- Metadati
            ip_address VARCHAR(45),
            user_agent TEXT,
            
            INDEX idx_operation_type (operation_type),
            INDEX idx_table_name (table_name),
            INDEX idx_job_id (job_id),
            INDEX idx_timestamp (operation_timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # Tabella per eventi di sicurezza
        security_events_sql = """
        CREATE TABLE IF NOT EXISTS security_events (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            
            event_type VARCHAR(100) NOT NULL,
            severity ENUM('low', 'medium', 'high', 'critical') NOT NULL,
            description TEXT NOT NULL,
            
            -- Contesto
            user_id VARCHAR(100),
            ip_address VARCHAR(45),
            user_agent TEXT,
            
            -- Dettagli evento
            event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            event_data JSON,
            
            -- Gestione
            acknowledged BOOLEAN DEFAULT FALSE,
            acknowledged_by VARCHAR(100),
            acknowledged_at TIMESTAMP NULL,
            
            INDEX idx_event_type (event_type),
            INDEX idx_severity (severity),
            INDEX idx_timestamp (event_timestamp),
            INDEX idx_acknowledged (acknowledged)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        self._execute_sql(audit_log_sql)
        self._execute_sql(security_events_sql)
        
        logger.info("✅ Tabelle audit create")
    
    def _register_schema_version(self) -> None:
        """Registra la versione corrente dello schema."""
        version_sql = """
        INSERT INTO schema_versions (version, description)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE applied_at = CURRENT_TIMESTAMP
        """
        
        self.db_connection.execute_with_retry(
            version_sql,
            (self.schema_version, "Schema enterprise ANAC Importer")
        )
        
        logger.info(f"✅ Schema versione {self.schema_version} registrata")
    
    def _execute_sql(self, sql: str, params: tuple = None) -> None:
        """Esegue una query SQL con gestione errori."""
        try:
            self.db_connection.execute_with_retry(sql, params)
        except Exception as e:
            logger.error(f"❌ Errore esecuzione SQL: {e}")
            logger.error(f"Query: {sql[:100]}...")
            raise
    
    def verify_schema_integrity(self) -> bool:
        """Verifica l'integrità dello schema database."""
        try:
            required_tables = [
                'schema_versions',
                'system_config', 
                'import_jobs',
                'import_batches',
                'job_messages',
                'anac_records_generic',
                'anac_aggiudicazioni',
                'anac_partecipanti',
                'audit_log',
                'security_events'
            ]
            
            # Verifica esistenza tabelle
            for table in required_tables:
                check_sql = """
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
                """
                
                result = self.db_connection.execute_with_retry(check_sql, (table,))
                if not result or result[0][0] == 0:
                    logger.error(f"❌ Tabella mancante: {table}")
                    return False
            
            logger.info("✅ Integrità schema verificata")
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore verifica integrità schema: {e}")
            return False
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Restituisce informazioni sullo schema corrente."""
        try:
            # Versione corrente
            version_sql = """
            SELECT version, applied_at 
            FROM schema_versions 
            ORDER BY applied_at DESC 
            LIMIT 1
            """
            
            version_result = self.db_connection.execute_with_retry(version_sql)
            current_version = version_result[0] if version_result else None
            
            # Conteggio tabelle
            tables_sql = """
            SELECT COUNT(*) as table_count
            FROM information_schema.tables 
            WHERE table_schema = DATABASE()
            """
            
            tables_result = self.db_connection.execute_with_retry(tables_sql)
            table_count = tables_result[0][0] if tables_result else 0
            
            return {
                'current_version': current_version[0] if current_version else 'unknown',
                'last_update': current_version[1] if current_version else None,
                'table_count': table_count,
                'schema_manager_version': self.schema_version
            }
            
        except Exception as e:
            logger.error(f"❌ Errore recupero info schema: {e}")
            return {}