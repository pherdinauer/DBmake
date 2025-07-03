#!/usr/bin/env python3
"""
ANAC Importer Enterprise Edition - Punto di ingresso principale.

Architettura enterprise con:
- Sicurezza di livello enterprise
- Transazioni ACID complete
- Zero data loss guarantee
- Monitoring e observability
- Gestione errori strutturata
"""

import sys
import os
import logging
from typing import List, Optional
from pathlib import Path
import click
from datetime import datetime

# Aggiungi src al path per gli import
sys.path.insert(0, str(Path(__file__).parent))

try:
    from src.security import SecureCredentialManager, SecureLogger
    from src.core import ImportService, ValidationLevel
    from src.database.secure_connection import SecureDatabaseConnection
    from src.core.exceptions import (
        ANACImporterError, SecurityError, ConfigurationError,
        ErrorSeverity, ErrorCategory
    )
except ImportError as e:
    print(f"🚨 ERRORE CRITICO: Impossibile importare moduli enterprise: {e}")
    print("Verifica che tutte le dipendenze siano installate correttamente.")
    sys.exit(1)

class EnterpriseImporter:
    """
    Classe principale per l'importazione enterprise ANAC.
    
    Gestisce l'intero ciclo di vita dell'applicazione con:
    - Inizializzazione sicura
    - Gestione credenziali enterprise
    - Monitoring completo
    - Recovery automatico
    """
    
    def __init__(self):
        self.credential_manager = None
        self.db_connection = None
        self.import_service = None
        self.logger = None
        
        # Stato dell'applicazione
        self.is_initialized = False
        self.startup_time = datetime.now()
    
    def initialize(self) -> bool:
        """
        Inizializzazione sicura dell'applicazione enterprise.
        
        Returns:
            True se inizializzazione riuscita
        """
        try:
            print("🚀 ANAC Importer Enterprise Edition - Inizializzazione")
            print("=" * 60)
            
            # 1. Setup logging sicuro
            self._setup_secure_logging()
            self.logger.info("🔐 Sistema di logging sicuro attivato")
            
            # 2. Gestione credenziali sicure
            self._setup_credentials()
            self.logger.info("✅ Credenziali sicure caricate")
            
            # 3. Connessione database sicura
            self._setup_database_connection()
            self.logger.info("🔗 Connessione database sicura stabilita")
            
            # 4. Test connettività e integrità
            self._verify_system_integrity()
            self.logger.info("🔍 Integrità sistema verificata")
            
            # 5. Setup servizi enterprise
            self._setup_enterprise_services()
            self.logger.info("⚙️ Servizi enterprise attivati")
            
            self.is_initialized = True
            duration = (datetime.now() - self.startup_time).total_seconds()
            
            self.logger.info(f"✅ Inizializzazione completata in {duration:.2f}s")
            print(f"✅ Sistema pronto per importazioni sicure")
            
            return True
            
        except SecurityError as e:
            print(f"🚨 ERRORE SICUREZZA CRITICO: {e.message}")
            if self.logger:
                self.logger.critical("Errore sicurezza durante inizializzazione", extra=e.to_dict())
            return False
            
        except Exception as e:
            print(f"❌ ERRORE INIZIALIZZAZIONE: {e}")
            if self.logger:
                self.logger.error(f"Errore inizializzazione: {e}")
            return False
    
    def _setup_secure_logging(self) -> None:
        """Setup del sistema di logging sicuro."""
        # Crea directory log se non esiste
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Logger sicuro con filtro automatico
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"anac_enterprise_{timestamp}.log"
        
        secure_logger = SecureLogger(
            name="anac_enterprise",
            log_file=str(log_file),
            level=logging.INFO
        )
        
        self.logger = secure_logger.get_logger()
        
        # Log evento sicurezza per startup
        secure_logger.log_security_event("application_startup", {
            "timestamp": self.startup_time.isoformat(),
            "version": "enterprise",
            "user": os.getenv("USER", "unknown")
        })
    
    def _setup_credentials(self) -> None:
        """Setup sicuro delle credenziali."""
        try:
            self.credential_manager = SecureCredentialManager()
            
            # Test recupero credenziali
            credentials = self.credential_manager.get_database_credentials()
            
            # Verifica che le credenziali siano sicure
            if not credentials or not all(credentials.get(k) for k in ['host', 'user', 'password', 'database']):
                raise SecurityError(
                    "Credenziali database incomplete o non sicure",
                    error_code="CREDENTIALS_INCOMPLETE"
                )
            
            self.db_credentials = credentials
            
        except Exception as e:
            raise SecurityError(
                f"Impossibile configurare credenziali sicure: {e}",
                error_code="CREDENTIALS_SETUP_FAILED"
            )
    
    def _setup_database_connection(self) -> None:
        """Setup connessione database sicura con pooling."""
        try:
            self.db_connection = SecureDatabaseConnection(
                credentials=self.db_credentials,
                pool_size=5
            )
            
            # Test connessione
            if not self.db_connection.test_connection():
                raise DatabaseError(
                    "Test connessione database fallito",
                    operation="connection_test"
                )
            
        except Exception as e:
            raise ConfigurationError(
                f"Impossibile stabilire connessione database sicura: {e}",
                config_key="database_connection"
            )
    
    def _verify_system_integrity(self) -> None:
        """Verifica l'integrità del sistema prima dell'avvio."""
        try:
            # Verifica spazio disco
            import shutil
            free_space_gb = shutil.disk_usage('.').free / (1024**3)
            if free_space_gb < 1.0:  # Minimo 1GB libero
                self.logger.warning(f"Spazio disco basso: {free_space_gb:.1f}GB")
            
            # Verifica memoria disponibile
            import psutil
            memory = psutil.virtual_memory()
            if memory.available < 512 * 1024 * 1024:  # Minimo 512MB
                self.logger.warning(f"Memoria bassa: {memory.available / (1024**2):.0f}MB")
            
            # Verifica permessi directory
            for directory in ['logs', 'database']:
                Path(directory).mkdir(exist_ok=True)
                if not os.access(directory, os.W_OK):
                    raise ConfigurationError(
                        f"Permessi scrittura mancanti per directory: {directory}",
                        config_key="directory_permissions"
                    )
            
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(
                f"Verifica integrità sistema fallita: {e}",
                config_key="system_integrity"
            )
    
    def _setup_enterprise_services(self) -> None:
        """Setup dei servizi enterprise."""
        try:
            self.import_service = ImportService(self.db_connection)
            
        except Exception as e:
            raise ConfigurationError(
                f"Impossibile inizializzare servizi enterprise: {e}",
                config_key="enterprise_services"
            )
    
    def execute_import(self, 
                      source_files: List[str],
                      job_name: str = None,
                      validation_level: str = "strict",
                      batch_size: int = 1000) -> bool:
        """
        Esegue un'importazione enterprise con garanzie complete.
        
        Args:
            source_files: Lista dei file da importare
            job_name: Nome del job (opzionale)
            validation_level: Livello di validazione (basic/strict/enterprise)
            batch_size: Dimensione dei batch
            
        Returns:
            True se importazione completata con successo
        """
        if not self.is_initialized:
            print("❌ Sistema non inizializzato. Esegui initialize() prima.")
            return False
        
        try:
            # Validazione input
            if not source_files:
                raise ValidationError("Nessun file sorgente specificato")
            
            # Verifica esistenza file
            missing_files = [f for f in source_files if not Path(f).exists()]
            if missing_files:
                raise ValidationError(f"File non trovati: {missing_files}")
            
            # Converti livello validazione
            validation_mapping = {
                'basic': ValidationLevel.BASIC,
                'strict': ValidationLevel.STRICT,
                'enterprise': ValidationLevel.ENTERPRISE
            }
            
            validation_level_enum = validation_mapping.get(validation_level.lower(), ValidationLevel.STRICT)
            
            # Nome job automatico se non specificato
            if not job_name:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                job_name = f"import_{timestamp}"
            
            self.logger.info(f"🚀 Avvio importazione enterprise: {job_name}")
            self.logger.info(f"📁 File: {len(source_files)}, Validazione: {validation_level}, Batch: {batch_size}")
            
            # Esegui importazione
            job = self.import_service.execute_import_job(
                source_files=source_files,
                job_name=job_name,
                validation_level=validation_level_enum,
                batch_size=batch_size
            )
            
            # Report risultati
            self._print_import_report(job)
            
            return job.integrity_verified and job.failed_records == 0
            
        except ANACImporterError as e:
            self.logger.error("Errore durante importazione", extra=e.to_dict())
            print(f"❌ ERRORE: {e.message}")
            return False
            
        except Exception as e:
            self.logger.error(f"Errore inaspettato durante importazione: {e}")
            print(f"❌ ERRORE INASPETTATO: {e}")
            return False
    
    def _print_import_report(self, job) -> None:
        """Stampa un report dettagliato dell'importazione."""
        print("\n" + "=" * 60)
        print("📊 REPORT IMPORTAZIONE ENTERPRISE")
        print("=" * 60)
        print(f"🆔 Job ID: {job.job_id}")
        print(f"📝 Nome: {job.name}")
        print(f"📊 Stato: {job.status.value.upper()}")
        print(f"⏱️  Durata: {job.get_duration():.2f}s" if job.get_duration() else "⏱️  Durata: N/A")
        print()
        print("📈 STATISTICHE:")
        print(f"   📄 Record totali: {job.total_records:,}")
        print(f"   ✅ Record processati: {job.processed_records:,}")
        print(f"   ✅ Record validi: {job.valid_records:,}")
        print(f"   ⚠️  Record non validi: {job.invalid_records:,}")
        print(f"   ❌ Record falliti: {job.failed_records:,}")
        print(f"   📊 Tasso successo: {job.get_success_rate():.1f}%")
        print()
        print("🔒 INTEGRITÀ:")
        print(f"   🔍 Verificata: {'✅ SÌ' if job.integrity_verified else '❌ NO'}")
        print(f"   📝 Checksum sorgente: {job.source_checksum[:16]}..." if job.source_checksum else "   📝 Checksum sorgente: N/A")
        print(f"   🎯 Checksum target: {job.target_checksum[:16]}..." if job.target_checksum else "   🎯 Checksum target: N/A")
        
        if job.errors:
            print("\n❌ ERRORI:")
            for error in job.errors[:5]:  # Mostra solo i primi 5
                print(f"   • {error}")
            if len(job.errors) > 5:
                print(f"   ... e altri {len(job.errors) - 5} errori")
        
        if job.warnings:
            print("\n⚠️  AVVISI:")
            for warning in job.warnings[:3]:  # Mostra solo i primi 3
                print(f"   • {warning}")
            if len(job.warnings) > 3:
                print(f"   ... e altri {len(job.warnings) - 3} avvisi")
        
        print("=" * 60)

# CLI Interface con Click
@click.group()
@click.version_option(version="1.0.0-enterprise")
def cli():
    """ANAC Importer Enterprise Edition - Sistema di importazione sicuro e affidabile."""
    pass

@cli.command()
@click.argument('files', nargs=-1, required=True)
@click.option('--job-name', '-n', help='Nome del job di importazione')
@click.option('--validation', '-v', 
              type=click.Choice(['basic', 'strict', 'enterprise'], case_sensitive=False),
              default='strict', help='Livello di validazione')
@click.option('--batch-size', '-b', type=int, default=1000, help='Dimensione dei batch')
@click.option('--verbose', is_flag=True, help='Output verboso')
def import_files(files, job_name, validation, batch_size, verbose):
    """Importa file JSON ANAC con garanzie enterprise."""
    
    # Setup logging level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Inizializza importer
    importer = EnterpriseImporter()
    
    print("🔧 Inizializzazione sistema enterprise...")
    if not importer.initialize():
        print("❌ Inizializzazione fallita. Impossibile continuare.")
        sys.exit(1)
    
    # Converti file paths
    file_list = [str(Path(f).resolve()) for f in files]
    
    print(f"\n📁 Importazione di {len(file_list)} file...")
    success = importer.execute_import(
        source_files=file_list,
        job_name=job_name,
        validation_level=validation,
        batch_size=batch_size
    )
    
    if success:
        print("\n✅ Importazione completata con successo!")
        sys.exit(0)
    else:
        print("\n❌ Importazione fallita!")
        sys.exit(1)

@cli.command()
def test_connection():
    """Testa la connessione al database."""
    print("🔧 Test connessione database...")
    
    importer = EnterpriseImporter()
    if importer.initialize():
        print("✅ Connessione database OK!")
    else:
        print("❌ Connessione database FALLITA!")
        sys.exit(1)

@cli.command()
def setup_credentials():
    """Setup interattivo delle credenziali."""
    print("🔐 Setup credenziali enterprise...")
    
    try:
        credential_manager = SecureCredentialManager()
        credentials = credential_manager.get_database_credentials()
        print("✅ Credenziali configurate con successo!")
    except Exception as e:
        print(f"❌ Errore configurazione credenziali: {e}")
        sys.exit(1)

if __name__ == "__main__":
    cli()