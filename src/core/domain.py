"""
Domain Layer - Entità di business e regole di dominio per ANAC Importer.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from enum import Enum
import hashlib
import json

class ImportStatus(Enum):
    """Stati possibili di un'importazione."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"

class ValidationLevel(Enum):
    """Livelli di validazione."""
    BASIC = "basic"
    STRICT = "strict"
    ENTERPRISE = "enterprise"

@dataclass
class ANACRecord:
    """
    Entità di dominio per un record ANAC.
    
    Rappresenta un singolo record con tutte le sue proprietà,
    validazioni e regole di business.
    """
    
    # Identificatori univoci
    cig: Optional[str] = None
    id_aggiudicazione: Optional[str] = None
    
    # Dati principali
    data: Dict[str, Any] = field(default_factory=dict)
    categoria: Optional[str] = None
    
    # Metadati
    source_file: Optional[str] = None
    import_timestamp: datetime = field(default_factory=datetime.now)
    checksum: Optional[str] = None
    
    # Stato di validazione
    is_valid: bool = True
    validation_errors: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Validazione e inizializzazione post-creazione."""
        if not self.checksum:
            self.checksum = self._calculate_checksum()
        
        # Estrai identificatori dai dati se non forniti
        if not self.cig and 'CIG' in self.data:
            self.cig = self.data['CIG']
        if not self.id_aggiudicazione and 'ID_AGGIUDICAZIONE' in self.data:
            self.id_aggiudicazione = self.data['ID_AGGIUDICAZIONE']
    
    def _calculate_checksum(self) -> str:
        """Calcola il checksum SHA256 dei dati."""
        data_string = json.dumps(self.data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(data_string.encode('utf-8')).hexdigest()
    
    def verify_integrity(self) -> bool:
        """Verifica l'integrità del record confrontando il checksum."""
        current_checksum = self._calculate_checksum()
        return current_checksum == self.checksum
    
    def get_unique_key(self) -> str:
        """Restituisce una chiave univoca per il record."""
        if self.cig and self.id_aggiudicazione:
            return f"{self.cig}_{self.id_aggiudicazione}"
        elif self.cig:
            return self.cig
        else:
            return self.checksum
    
    def add_validation_error(self, error: str) -> None:
        """Aggiunge un errore di validazione."""
        self.validation_errors.append(error)
        self.is_valid = False
    
    def clear_validation_errors(self) -> None:
        """Pulisce gli errori di validazione."""
        self.validation_errors.clear()
        self.is_valid = True

@dataclass
class ValidationResult:
    """Risultato di una validazione."""
    
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    sanitized_data: Optional[Dict[str, Any]] = None
    validation_level: ValidationLevel = ValidationLevel.BASIC
    
    def add_error(self, error: str) -> None:
        """Aggiunge un errore di validazione."""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str) -> None:
        """Aggiunge un avviso di validazione."""
        self.warnings.append(warning)
    
    def merge(self, other: 'ValidationResult') -> None:
        """Unisce i risultati di un'altra validazione."""
        if not other.is_valid:
            self.is_valid = False
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)

@dataclass
class ImportJob:
    """
    Entità di dominio per un job di importazione.
    
    Gestisce l'intero ciclo di vita di un'importazione con
    tracking completo e garanzie ACID.
    """
    
    # Identificatori
    job_id: str
    name: str
    
    # Configurazione
    source_files: List[str] = field(default_factory=list)
    target_tables: List[str] = field(default_factory=list)
    validation_level: ValidationLevel = ValidationLevel.STRICT
    
    # Stato
    status: ImportStatus = ImportStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Statistiche
    total_records: int = 0
    processed_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    failed_records: int = 0
    
    # Integrità
    source_checksum: Optional[str] = None
    target_checksum: Optional[str] = None
    integrity_verified: bool = False
    
    # Errori e log
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def start(self) -> None:
        """Avvia il job di importazione."""
        self.status = ImportStatus.RUNNING
        self.started_at = datetime.now()
    
    def complete(self) -> None:
        """Completa il job di importazione."""
        self.status = ImportStatus.COMPLETED
        self.completed_at = datetime.now()
    
    def fail(self, error: str) -> None:
        """Marca il job come fallito."""
        self.status = ImportStatus.FAILED
        self.completed_at = datetime.now()
        self.errors.append(error)
    
    def rollback(self, reason: str) -> None:
        """Marca il job come rollback."""
        self.status = ImportStatus.ROLLED_BACK
        self.completed_at = datetime.now()
        self.errors.append(f"Rollback: {reason}")
    
    def add_processed_record(self, is_valid: bool = True) -> None:
        """Aggiunge un record processato alle statistiche."""
        self.processed_records += 1
        if is_valid:
            self.valid_records += 1
        else:
            self.invalid_records += 1
    
    def add_failed_record(self, error: str) -> None:
        """Aggiunge un record fallito."""
        self.failed_records += 1
        self.errors.append(error)
    
    def get_success_rate(self) -> float:
        """Calcola il tasso di successo."""
        if self.processed_records == 0:
            return 0.0
        return (self.valid_records / self.processed_records) * 100
    
    def get_duration(self) -> Optional[float]:
        """Calcola la durata del job in secondi."""
        if not self.started_at:
            return None
        end_time = self.completed_at or datetime.now()
        return (end_time - self.started_at).total_seconds()
    
    def verify_integrity(self) -> bool:
        """Verifica l'integrità dell'importazione."""
        # Implementazione base - può essere estesa
        return (
            self.source_checksum is not None and
            self.target_checksum is not None and
            self.source_checksum == self.target_checksum
        )

@dataclass
class ImportBatch:
    """
    Rappresenta un batch di record da importare.
    Gestisce transazioni ACID a livello di batch.
    """
    
    batch_id: str
    job_id: str
    records: List[ANACRecord] = field(default_factory=list)
    
    # Stato del batch
    status: ImportStatus = ImportStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    processed_at: Optional[datetime] = None
    
    # Integrità
    checksum: Optional[str] = None
    
    def __post_init__(self):
        """Calcola il checksum del batch."""
        if not self.checksum:
            self.checksum = self._calculate_batch_checksum()
    
    def _calculate_batch_checksum(self) -> str:
        """Calcola il checksum dell'intero batch."""
        batch_data = {
            'batch_id': self.batch_id,
            'job_id': self.job_id,
            'records': [record.checksum for record in self.records]
        }
        data_string = json.dumps(batch_data, sort_keys=True)
        return hashlib.sha256(data_string.encode('utf-8')).hexdigest()
    
    def add_record(self, record: ANACRecord) -> None:
        """Aggiunge un record al batch e ricalcola il checksum."""
        self.records.append(record)
        self.checksum = self._calculate_batch_checksum()
    
    def get_size(self) -> int:
        """Restituisce il numero di record nel batch."""
        return len(self.records)
    
    def get_valid_records(self) -> List[ANACRecord]:
        """Restituisce solo i record validi."""
        return [record for record in self.records if record.is_valid]
    
    def get_invalid_records(self) -> List[ANACRecord]:
        """Restituisce solo i record non validi."""
        return [record for record in self.records if not record.is_valid]