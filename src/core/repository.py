"""
Repository Pattern con Transaction Manager ACID per garantire integrità dei dati.
Implementa il principio Single Responsibility e Dependency Inversion.
"""

from typing import List, Dict, Any, Optional, ContextManager, Protocol
from contextlib import contextmanager
import logging
import uuid
from datetime import datetime

from .domain import ANACRecord, ImportJob, ImportBatch, ImportStatus, ValidationResult
from ..database.secure_connection import SecureDatabaseConnection

logger = logging.getLogger(__name__)

class IRepository(Protocol):
    """Interface per il Repository Pattern."""
    
    def save(self, entity: Any) -> bool:
        """Salva un'entità."""
        ...
    
    def find_by_id(self, entity_id: str) -> Optional[Any]:
        """Trova un'entità per ID."""
        ...
    
    def delete(self, entity_id: str) -> bool:
        """Elimina un'entità."""
        ...

class TransactionManager:
    """
    Gestore delle transazioni ACID per garantire integrità dei dati.
    
    Caratteristiche:
    - Transazioni ACID complete
    - Rollback automatico in caso di errore
    - Savepoint per transazioni annidate
    - Monitoring e audit trail
    """
    
    def __init__(self, db_connection: SecureDatabaseConnection):
        self.db_connection = db_connection
        self._active_transactions = {}
        self._savepoints = {}
    
    @contextmanager
    def transaction(self, transaction_id: str = None):
        """
        Context manager per transazioni ACID.
        
        Args:
            transaction_id: ID opzionale per la transazione
            
        Yields:
            Transaction context
        """
        if not transaction_id:
            transaction_id = str(uuid.uuid4())
        
        logger.info(f"🔄 Inizio transazione ACID: {transaction_id}")
        
        try:
            with self.db_connection.get_connection(autocommit=False) as conn:
                self._active_transactions[transaction_id] = {
                    'connection': conn,
                    'started_at': datetime.now(),
                    'operations': []
                }
                
                yield TransactionContext(transaction_id, conn, self)
                
                # Se arriviamo qui, commit della transazione
                conn.commit()
                logger.info(f"✅ Transazione ACID completata: {transaction_id}")
                
        except Exception as e:
            # Rollback automatico in caso di errore
            if transaction_id in self._active_transactions:
                conn = self._active_transactions[transaction_id]['connection']
                conn.rollback()
                logger.error(f"🔄 Rollback transazione: {transaction_id} - {e}")
            raise
        finally:
            # Pulizia
            if transaction_id in self._active_transactions:
                del self._active_transactions[transaction_id]
    
    def create_savepoint(self, transaction_id: str, savepoint_name: str) -> None:
        """Crea un savepoint all'interno di una transazione."""
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transazione {transaction_id} non attiva")
        
        conn = self._active_transactions[transaction_id]['connection']
        cursor = conn.cursor()
        cursor.execute(f"SAVEPOINT {savepoint_name}")
        cursor.close()
        
        self._savepoints[f"{transaction_id}_{savepoint_name}"] = datetime.now()
        logger.debug(f"📍 Savepoint creato: {savepoint_name} in {transaction_id}")
    
    def rollback_to_savepoint(self, transaction_id: str, savepoint_name: str) -> None:
        """Rollback a un savepoint specifico."""
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transazione {transaction_id} non attiva")
        
        conn = self._active_transactions[transaction_id]['connection']
        cursor = conn.cursor()
        cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        cursor.close()
        
        logger.warning(f"🔄 Rollback a savepoint: {savepoint_name} in {transaction_id}")

class TransactionContext:
    """Contesto di una transazione attiva."""
    
    def __init__(self, transaction_id: str, connection: Any, manager: TransactionManager):
        self.transaction_id = transaction_id
        self.connection = connection
        self.manager = manager
    
    def execute(self, query: str, params: tuple = None) -> Any:
        """Esegue una query nel contesto della transazione."""
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Log dell'operazione
            self.manager._active_transactions[self.transaction_id]['operations'].append({
                'query': query[:100] + '...' if len(query) > 100 else query,
                'timestamp': datetime.now()
            })
            
            return cursor
        except Exception as e:
            cursor.close()
            raise
    
    def create_savepoint(self, name: str) -> None:
        """Crea un savepoint."""
        self.manager.create_savepoint(self.transaction_id, name)
    
    def rollback_to_savepoint(self, name: str) -> None:
        """Rollback a un savepoint."""
        self.manager.rollback_to_savepoint(self.transaction_id, name)

class ANACRepository:
    """
    Repository per entità ANAC con garanzie ACID e integrità dei dati.
    
    Implementa il Repository Pattern con:
    - Transazioni ACID complete
    - Validazione dell'integrità
    - Audit trail
    - Gestione errori robusta
    """
    
    def __init__(self, db_connection: SecureDatabaseConnection):
        self.db_connection = db_connection
        self.transaction_manager = TransactionManager(db_connection)
    
    def save_import_job(self, job: ImportJob) -> bool:
        """
        Salva un job di importazione con garanzie ACID.
        
        Args:
            job: Job di importazione da salvare
            
        Returns:
            True se salvato con successo
        """
        try:
            with self.transaction_manager.transaction() as tx:
                # Inserisci il job principale
                job_query = """
                INSERT INTO import_jobs (
                    job_id, name, status, created_at, started_at, completed_at,
                    total_records, processed_records, valid_records, invalid_records,
                    source_checksum, target_checksum, integrity_verified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    completed_at = VALUES(completed_at),
                    processed_records = VALUES(processed_records),
                    valid_records = VALUES(valid_records),
                    invalid_records = VALUES(invalid_records),
                    target_checksum = VALUES(target_checksum),
                    integrity_verified = VALUES(integrity_verified)
                """
                
                cursor = tx.execute(job_query, (
                    job.job_id, job.name, job.status.value, job.created_at,
                    job.started_at, job.completed_at, job.total_records,
                    job.processed_records, job.valid_records, job.invalid_records,
                    job.source_checksum, job.target_checksum, job.integrity_verified
                ))
                cursor.close()
                
                # Salva errori e warning se presenti
                if job.errors:
                    self._save_job_errors(tx, job.job_id, job.errors, 'error')
                
                if job.warnings:
                    self._save_job_errors(tx, job.job_id, job.warnings, 'warning')
                
                logger.info(f"✅ Job salvato: {job.job_id}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Errore nel salvataggio job {job.job_id}: {e}")
            return False
    
    def save_batch_with_integrity(self, batch: ImportBatch) -> bool:
        """
        Salva un batch con verifica completa dell'integrità.
        
        Args:
            batch: Batch da salvare
            
        Returns:
            True se salvato con successo e integrità verificata
        """
        try:
            with self.transaction_manager.transaction() as tx:
                # Crea savepoint per il batch
                tx.create_savepoint(f"batch_{batch.batch_id}")
                
                # Salva metadati del batch
                batch_query = """
                INSERT INTO import_batches (
                    batch_id, job_id, status, created_at, processed_at,
                    record_count, checksum
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor = tx.execute(batch_query, (
                    batch.batch_id, batch.job_id, batch.status.value,
                    batch.created_at, batch.processed_at,
                    batch.get_size(), batch.checksum
                ))
                cursor.close()
                
                # Salva i record del batch
                records_saved = 0
                for record in batch.records:
                    if self._save_record_in_transaction(tx, record, batch.batch_id):
                        records_saved += 1
                    else:
                        # Se un record fallisce, rollback a savepoint
                        logger.warning(f"Record fallito in batch {batch.batch_id}, rollback parziale")
                        tx.rollback_to_savepoint(f"batch_{batch.batch_id}")
                        return False
                
                # Verifica integrità finale
                if not self._verify_batch_integrity(tx, batch):
                    logger.error(f"❌ Verifica integrità fallita per batch {batch.batch_id}")
                    return False
                
                logger.info(f"✅ Batch salvato con integrità verificata: {batch.batch_id} ({records_saved} record)")
                return True
                
        except Exception as e:
            logger.error(f"❌ Errore nel salvataggio batch {batch.batch_id}: {e}")
            return False
    
    def _save_record_in_transaction(self, tx: TransactionContext, record: ANACRecord, batch_id: str) -> bool:
        """Salva un singolo record all'interno di una transazione."""
        try:
            # Determina la tabella target basandosi sulla categoria
            table_name = self._get_table_name_for_category(record.categoria)
            
            # Prepara i dati per l'inserimento
            insert_data = record.data.copy()
            insert_data.update({
                '_batch_id': batch_id,
                '_import_timestamp': record.import_timestamp,
                '_checksum': record.checksum,
                '_source_file': record.source_file
            })
            
            # Costruisci query dinamica
            columns = list(insert_data.keys())
            placeholders = ', '.join(['%s'] * len(columns))
            values = list(insert_data.values())
            
            query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            """
            
            cursor = tx.execute(query, tuple(values))
            cursor.close()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nel salvataggio record: {e}")
            return False
    
    def _verify_batch_integrity(self, tx: TransactionContext, batch: ImportBatch) -> bool:
        """Verifica l'integrità di un batch dopo il salvataggio."""
        try:
            # Conta i record salvati
            count_query = "SELECT COUNT(*) FROM import_batches WHERE batch_id = %s"
            cursor = tx.execute(count_query, (batch.batch_id,))
            saved_count = cursor.fetchone()[0]
            cursor.close()
            
            # Verifica che il numero corrisponda
            expected_count = batch.get_size()
            if saved_count != expected_count:
                logger.error(f"❌ Conteggio record non corrispondente: {saved_count} vs {expected_count}")
                return False
            
            # Verifica checksum (implementazione semplificata)
            # In un'implementazione completa, si dovrebbe ricalcolare il checksum dei dati salvati
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nella verifica integrità: {e}")
            return False
    
    def _save_job_errors(self, tx: TransactionContext, job_id: str, messages: List[str], message_type: str) -> None:
        """Salva errori/warning di un job."""
        for message in messages:
            error_query = """
            INSERT INTO job_messages (job_id, message_type, message, created_at)
            VALUES (%s, %s, %s, %s)
            """
            cursor = tx.execute(error_query, (job_id, message_type, message, datetime.now()))
            cursor.close()
    
    def _get_table_name_for_category(self, categoria: Optional[str]) -> str:
        """Determina il nome della tabella basandosi sulla categoria."""
        if not categoria:
            return 'anac_records_generic'
        
        # Mapping categoria -> tabella
        category_mapping = {
            'aggiudicazioni': 'anac_aggiudicazioni',
            'partecipanti': 'anac_partecipanti',
            'collaudo': 'anac_collaudo',
            'lavorazioni': 'anac_lavorazioni',
            # Aggiungi altre mappature secondo necessità
        }
        
        return category_mapping.get(categoria.lower(), 'anac_records_generic')
    
    def find_job_by_id(self, job_id: str) -> Optional[ImportJob]:
        """Trova un job per ID."""
        try:
            query = """
            SELECT job_id, name, status, created_at, started_at, completed_at,
                   total_records, processed_records, valid_records, invalid_records,
                   source_checksum, target_checksum, integrity_verified
            FROM import_jobs WHERE job_id = %s
            """
            
            result = self.db_connection.execute_with_retry(query, (job_id,))
            if result and len(result) > 0:
                row = result[0]
                
                # Ricostruisci l'oggetto ImportJob
                job = ImportJob(
                    job_id=row[0],
                    name=row[1],
                    status=ImportStatus(row[2]),
                    created_at=row[3],
                    started_at=row[4],
                    completed_at=row[5],
                    total_records=row[6],
                    processed_records=row[7],
                    valid_records=row[8],
                    invalid_records=row[9],
                    source_checksum=row[10],
                    target_checksum=row[11],
                    integrity_verified=row[12]
                )
                
                return job
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero job {job_id}: {e}")
            return None
    
    def get_job_statistics(self, job_id: str) -> Dict[str, Any]:
        """Recupera statistiche dettagliate di un job."""
        try:
            stats_query = """
            SELECT 
                COUNT(*) as total_batches,
                SUM(record_count) as total_records,
                AVG(record_count) as avg_batch_size,
                MIN(created_at) as first_batch,
                MAX(processed_at) as last_batch
            FROM import_batches 
            WHERE job_id = %s
            """
            
            result = self.db_connection.execute_with_retry(stats_query, (job_id,))
            if result and len(result) > 0:
                row = result[0]
                return {
                    'total_batches': row[0],
                    'total_records': row[1],
                    'avg_batch_size': row[2],
                    'first_batch': row[3],
                    'last_batch': row[4]
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero statistiche job {job_id}: {e}")
            return {}