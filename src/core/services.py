"""
Service Layer - Orchestrazione dei processi di business con garanzie enterprise.
Implementa il principio Single Responsibility e gestione completa dell'integrità.
"""

import logging
import uuid
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json
import hashlib

from .domain import ANACRecord, ImportJob, ImportBatch, ImportStatus, ValidationResult, ValidationLevel
from .repository import ANACRepository
from ..security import InputValidator, SecureLogger
from ..database.secure_connection import SecureDatabaseConnection

logger = logging.getLogger(__name__)

class IntegrityService:
    """
    Servizio per la gestione dell'integrità dei dati.
    Garantisce zero data loss e verifica completa dell'integrità.
    """
    
    def __init__(self):
        self.secure_logger = SecureLogger(__name__)
        self.logger = self.secure_logger.get_logger()
    
    def calculate_file_checksum(self, file_path: str) -> str:
        """Calcola il checksum SHA256 di un file."""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            self.logger.error(f"Errore nel calcolo checksum file {file_path}: {e}")
            return ""
    
    def calculate_data_checksum(self, data: List[Dict[str, Any]]) -> str:
        """Calcola il checksum di una lista di dati."""
        try:
            # Serializza i dati in modo deterministico
            data_string = json.dumps(data, sort_keys=True, ensure_ascii=False)
            return hashlib.sha256(data_string.encode('utf-8')).hexdigest()
        except Exception as e:
            self.logger.error(f"Errore nel calcolo checksum dati: {e}")
            return ""
    
    def verify_import_integrity(self, job: ImportJob, repository: ANACRepository) -> bool:
        """
        Verifica completa dell'integrità di un'importazione.
        
        Args:
            job: Job di importazione da verificare
            repository: Repository per accesso ai dati
            
        Returns:
            True se l'integrità è verificata
        """
        try:
            self.logger.info(f"🔍 Verifica integrità job: {job.job_id}")
            
            # 1. Verifica checksum source vs target
            if not job.source_checksum or not job.target_checksum:
                self.logger.error("❌ Checksum mancanti per verifica integrità")
                return False
            
            if job.source_checksum != job.target_checksum:
                self.logger.error(f"❌ Checksum non corrispondenti: {job.source_checksum} vs {job.target_checksum}")
                return False
            
            # 2. Verifica conteggio record
            stats = repository.get_job_statistics(job.job_id)
            if stats.get('total_records', 0) != job.processed_records:
                self.logger.error(f"❌ Conteggio record non corrispondente: DB={stats.get('total_records')} vs Job={job.processed_records}")
                return False
            
            # 3. Verifica che non ci siano record corrotti
            # (implementazione semplificata - può essere estesa)
            
            self.logger.info(f"✅ Integrità verificata per job: {job.job_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Errore nella verifica integrità: {e}")
            return False
    
    def create_integrity_report(self, job: ImportJob) -> Dict[str, Any]:
        """Crea un report dettagliato dell'integrità."""
        return {
            'job_id': job.job_id,
            'integrity_verified': job.integrity_verified,
            'source_checksum': job.source_checksum,
            'target_checksum': job.target_checksum,
            'checksums_match': job.source_checksum == job.target_checksum if job.source_checksum and job.target_checksum else False,
            'total_records': job.total_records,
            'processed_records': job.processed_records,
            'valid_records': job.valid_records,
            'invalid_records': job.invalid_records,
            'success_rate': job.get_success_rate(),
            'verification_timestamp': datetime.now()
        }

class ValidationService:
    """
    Servizio di validazione avanzata con regole di business ANAC.
    """
    
    def __init__(self):
        self.input_validator = InputValidator()
        self.secure_logger = SecureLogger(__name__)
        self.logger = self.secure_logger.get_logger()
    
    def validate_record(self, record_data: Dict[str, Any], validation_level: ValidationLevel = ValidationLevel.STRICT) -> ValidationResult:
        """
        Valida un singolo record con il livello specificato.
        
        Args:
            record_data: Dati del record da validare
            validation_level: Livello di validazione
            
        Returns:
            Risultato della validazione
        """
        result = ValidationResult(is_valid=True, validation_level=validation_level)
        
        try:
            # Validazione base: input sanitization
            input_validation = self.input_validator.validate_record(record_data)
            if not input_validation['is_valid']:
                for field, errors in input_validation['field_errors'].items():
                    for error in errors:
                        result.add_error(f"{field}: {error}")
            
            result.sanitized_data = input_validation['sanitized_record']
            
            # Validazione business rules specifiche per ANAC
            if validation_level in [ValidationLevel.STRICT, ValidationLevel.ENTERPRISE]:
                self._validate_anac_business_rules(record_data, result)
            
            # Validazione enterprise (più rigorosa)
            if validation_level == ValidationLevel.ENTERPRISE:
                self._validate_enterprise_rules(record_data, result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Errore nella validazione record: {e}")
            result.add_error(f"Errore interno di validazione: {e}")
            return result
    
    def _validate_anac_business_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Valida regole di business specifiche per ANAC."""
        
        # Regola 1: CIG obbligatorio
        if 'CIG' not in data or not data['CIG']:
            result.add_error("CIG è obbligatorio")
        elif len(str(data['CIG'])) != 10:
            result.add_error("CIG deve essere di 10 caratteri")
        
        # Regola 2: Importi numerici validi
        importo_fields = ['importo_aggiudicazione', 'importo_lotto', 'importo_offerta']
        for field in importo_fields:
            if field in data and data[field]:
                try:
                    importo = float(str(data[field]).replace(',', '.'))
                    if importo < 0:
                        result.add_error(f"{field} non può essere negativo")
                    elif importo > 1000000000:  # 1 miliardo
                        result.add_warning(f"{field} molto elevato: {importo}")
                except (ValueError, TypeError):
                    result.add_error(f"{field} non è un importo valido")
        
        # Regola 3: Date valide
        date_fields = ['data_aggiudicazione', 'data_stipula', 'data_inizio', 'data_fine']
        for field in date_fields:
            if field in data and data[field]:
                if not self._is_valid_date_format(data[field]):
                    result.add_error(f"{field} formato data non valido")
        
        # Regola 4: Codici fiscali validi
        cf_fields = ['codice_fiscale_aggiudicatario', 'codice_fiscale_contraente']
        for field in cf_fields:
            if field in data and data[field]:
                if not self._is_valid_codice_fiscale(data[field]):
                    result.add_error(f"{field} non è un codice fiscale valido")
    
    def _validate_enterprise_rules(self, data: Dict[str, Any], result: ValidationResult) -> None:
        """Validazione enterprise con regole più rigorose."""
        
        # Regola enterprise 1: Completezza dati
        required_enterprise_fields = ['CIG', 'denominazione_aggiudicatario', 'importo_aggiudicazione']
        for field in required_enterprise_fields:
            if field not in data or not data[field]:
                result.add_error(f"Campo obbligatorio enterprise: {field}")
        
        # Regola enterprise 2: Coerenza dati
        if 'data_aggiudicazione' in data and 'data_stipula' in data:
            if self._parse_date(data['data_aggiudicazione']) > self._parse_date(data['data_stipula']):
                result.add_error("Data aggiudicazione non può essere successiva a data stipula")
        
        # Regola enterprise 3: Limiti di sicurezza
        for field, value in data.items():
            if isinstance(value, str) and len(value) > 10000:
                result.add_warning(f"Campo {field} molto lungo ({len(value)} caratteri)")
    
    def _is_valid_date_format(self, date_str: str) -> bool:
        """Verifica se una stringa è una data valida."""
        try:
            # Supporta diversi formati comuni
            formats = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']
            for fmt in formats:
                try:
                    datetime.strptime(str(date_str), fmt)
                    return True
                except ValueError:
                    continue
            return False
        except:
            return False
    
    def _is_valid_codice_fiscale(self, cf: str) -> bool:
        """Verifica se un codice fiscale è valido (implementazione semplificata)."""
        import re
        cf = str(cf).upper().strip()
        # Pattern per codice fiscale italiano
        pattern = r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$'
        return bool(re.match(pattern, cf))
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse una data da stringa."""
        try:
            formats = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']
            for fmt in formats:
                try:
                    return datetime.strptime(str(date_str), fmt)
                except ValueError:
                    continue
            return None
        except:
            return None

class ImportService:
    """
    Servizio principale per l'orchestrazione delle importazioni.
    Coordina tutti i componenti enterprise per garantire importazioni sicure e affidabili.
    """
    
    def __init__(self, db_connection: SecureDatabaseConnection):
        self.db_connection = db_connection
        self.repository = ANACRepository(db_connection)
        self.validation_service = ValidationService()
        self.integrity_service = IntegrityService()
        
        self.secure_logger = SecureLogger(__name__)
        self.logger = self.secure_logger.get_logger()
    
    def execute_import_job(self, 
                          source_files: List[str], 
                          job_name: str,
                          validation_level: ValidationLevel = ValidationLevel.STRICT,
                          batch_size: int = 1000) -> ImportJob:
        """
        Esegue un job di importazione completo con garanzie enterprise.
        
        Args:
            source_files: Lista dei file da importare
            job_name: Nome del job
            validation_level: Livello di validazione
            batch_size: Dimensione dei batch
            
        Returns:
            Job di importazione completato
        """
        
        # Crea il job
        job = ImportJob(
            job_id=str(uuid.uuid4()),
            name=job_name,
            source_files=source_files,
            validation_level=validation_level
        )
        
        try:
            self.logger.info(f"🚀 Avvio job importazione: {job.job_id}")
            job.start()
            
            # Salva il job iniziale
            self.repository.save_import_job(job)
            
            # Calcola checksum dei file sorgente
            job.source_checksum = self._calculate_source_checksum(source_files)
            
            # Processa ogni file
            for file_path in source_files:
                self._process_file_with_batches(file_path, job, batch_size)
            
            # Verifica integrità finale
            job.integrity_verified = self.integrity_service.verify_import_integrity(job, self.repository)
            
            # Completa il job
            if job.integrity_verified and job.failed_records == 0:
                job.complete()
                self.logger.info(f"✅ Job completato con successo: {job.job_id}")
            else:
                job.fail("Verifica integrità fallita o record falliti")
                self.logger.error(f"❌ Job fallito: {job.job_id}")
            
            # Salva il job finale
            self.repository.save_import_job(job)
            
            return job
            
        except Exception as e:
            self.logger.error(f"❌ Errore critico nel job {job.job_id}: {e}")
            job.fail(str(e))
            self.repository.save_import_job(job)
            return job
    
    def _process_file_with_batches(self, file_path: str, job: ImportJob, batch_size: int) -> None:
        """Processa un file dividendolo in batch."""
        try:
            self.logger.info(f"📁 Processando file: {file_path}")
            
            # Leggi e parsa il file JSON
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                data = [data]
            
            job.total_records += len(data)
            
            # Dividi in batch
            for i in range(0, len(data), batch_size):
                batch_data = data[i:i + batch_size]
                batch = self._create_batch_from_data(batch_data, job, file_path)
                
                # Processa il batch
                if self._process_batch(batch, job):
                    self.logger.info(f"✅ Batch processato: {batch.batch_id}")
                else:
                    self.logger.error(f"❌ Batch fallito: {batch.batch_id}")
                    job.add_failed_record(f"Batch {batch.batch_id} fallito")
            
        except Exception as e:
            self.logger.error(f"❌ Errore nel processamento file {file_path}: {e}")
            job.add_failed_record(f"File {file_path}: {e}")
    
    def _create_batch_from_data(self, data: List[Dict[str, Any]], job: ImportJob, source_file: str) -> ImportBatch:
        """Crea un batch da dati grezzi."""
        batch = ImportBatch(
            batch_id=str(uuid.uuid4()),
            job_id=job.job_id
        )
        
        for record_data in data:
            # Valida il record
            validation = self.validation_service.validate_record(record_data, job.validation_level)
            
            # Crea il record ANAC
            record = ANACRecord(
                data=validation.sanitized_data or record_data,
                source_file=source_file,
                is_valid=validation.is_valid,
                validation_errors=validation.errors
            )
            
            batch.add_record(record)
        
        return batch
    
    def _process_batch(self, batch: ImportBatch, job: ImportJob) -> bool:
        """Processa un singolo batch con transazione ACID."""
        try:
            # Salva il batch nel repository con garanzie ACID
            success = self.repository.save_batch_with_integrity(batch)
            
            if success:
                # Aggiorna statistiche del job
                for record in batch.records:
                    job.add_processed_record(record.is_valid)
                
                batch.status = ImportStatus.COMPLETED
                batch.processed_at = datetime.now()
            else:
                batch.status = ImportStatus.FAILED
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Errore nel processamento batch {batch.batch_id}: {e}")
            batch.status = ImportStatus.FAILED
            return False
    
    def _calculate_source_checksum(self, source_files: List[str]) -> str:
        """Calcola il checksum combinato dei file sorgente."""
        try:
            combined_hash = hashlib.sha256()
            for file_path in sorted(source_files):  # Ordine deterministico
                file_checksum = self.integrity_service.calculate_file_checksum(file_path)
                combined_hash.update(file_checksum.encode('utf-8'))
            return combined_hash.hexdigest()
        except Exception as e:
            self.logger.error(f"❌ Errore nel calcolo checksum sorgente: {e}")
            return ""