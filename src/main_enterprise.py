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
def setup_wizard():
    """🧙‍♂️ Avvia il wizard di setup interattivo completo."""
    try:
        from src.cli.wizard import SetupWizard
        
        wizard = SetupWizard()
        success = wizard.run_wizard()
        
        if success:
            print("✅ Setup completato con successo!")
            sys.exit(0)
        else:
            print("❌ Setup fallito!")
            sys.exit(1)
            
    except ImportError as e:
        print(f"🚨 Wizard non disponibile: {e}")
        print("Installa le dipendenze: pip install rich click mysql-connector-python")
        sys.exit(1)
    except Exception as e:
        print(f"🚨 Errore durante setup: {e}")
        sys.exit(1)

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

@cli.command()
def status():
    """📊 Mostra lo stato del sistema e configurazione."""
    print("📊 ANAC Importer Enterprise - Status Sistema")
    print("=" * 50)
    
    try:
        # Test moduli
        print("📦 MODULI:")
        try:
            from src.security import SecureCredentialManager
            print("   ✅ Security Module: OK")
        except ImportError:
            print("   ❌ Security Module: ERRORE")
        
        try:
            from src.database.secure_connection import SecureDatabaseConnection
            print("   ✅ Database Module: OK")
        except ImportError:
            print("   ❌ Database Module: ERRORE")
        
        try:
            from src.core import ImportService
            print("   ✅ Core Module: OK")
        except ImportError:
            print("   ❌ Core Module: ERRORE")
        
        # Test credenziali
        print("\n🔑 CREDENZIALI:")
        try:
            credential_manager = SecureCredentialManager()
            credentials = credential_manager.get_database_credentials()
            print("   ✅ Credenziali: Disponibili")
            print(f"   🌐 Host: {credentials.get('host', 'N/A')}")
            print(f"   👤 User: {credentials.get('user', 'N/A')}")
            print(f"   🗄️ Database: {credentials.get('database', 'N/A')}")
        except Exception:
            print("   ❌ Credenziali: Non configurate")
            print("   💡 Esegui: python3 src/main_enterprise.py setup-wizard")
        
        # Test connessione database
        print("\n🔗 CONNESSIONE DATABASE:")
        try:
            importer = EnterpriseImporter()
            importer._setup_credentials()
            importer._setup_database_connection()
            
            if importer.db_connection.test_connection():
                print("   ✅ Connessione: OK")
                
                # Info schema
                try:
                    from src.database.schema_manager import SchemaManager
                    schema_manager = SchemaManager(importer.db_connection)
                    schema_info = schema_manager.get_schema_info()
                    print(f"   📊 Schema Version: {schema_info.get('current_version', 'N/A')}")
                    print(f"   📋 Tabelle: {schema_info.get('table_count', 'N/A')}")
                except Exception:
                    print("   ⚠️ Info schema non disponibili")
            else:
                print("   ❌ Connessione: FALLITA")
        except Exception as e:
            print(f"   ❌ Connessione: ERRORE - {e}")
        
        # Directories
        print("\n📁 DIRECTORIES:")
        directories = ['logs', 'database', 'demo']
        for directory in directories:
            path = Path(directory)
            if path.exists():
                print(f"   ✅ {directory}/: Presente")
            else:
                print(f"   ⚠️ {directory}/: Mancante")
        
        print("\n" + "=" * 50)
        
    except Exception as e:
        print(f"❌ Errore nel controllo status: {e}")
        sys.exit(1)

@cli.command()
def quickstart():
    """🚀 Guida rapida per iniziare."""
    quickstart_text = """
🚀 ANAC Importer Enterprise - Guida Rapida

PRIMO SETUP:
1. Esegui il wizard di configurazione:
   python3 src/main_enterprise.py setup-wizard

2. Il wizard ti guiderà attraverso:
   • Configurazione credenziali database
   • Creazione database (se necessario)
   • Inizializzazione schema enterprise
   • Test completo del sistema

UTILIZZO QUOTIDIANO:
1. Verifica sistema:
   python3 src/main_enterprise.py status

2. Test connessione:
   python3 src/main_enterprise.py test-connection

3. Importa file ANAC:
   python3 src/main_enterprise.py import-files file1.json file2.json

OPZIONI AVANZATE:
• Validazione enterprise:
  python3 src/main_enterprise.py import-files --validation enterprise file.json

• Batch personalizzato:
  python3 src/main_enterprise.py import-files --batch-size 2000 file.json

• Output verboso:
  python3 src/main_enterprise.py import-files --verbose file.json

AIUTO:
• Lista comandi: python3 src/main_enterprise.py --help
• Help comando: python3 src/main_enterprise.py COMANDO --help

DOCUMENTAZIONE:
• Guida completa: cat IMPLEMENTAZIONE_COMPLETA_ENTERPRISE.md
• Demo sistema: python3 demo/enterprise_demo.py

🎯 Per problemi: Controlla sempre prima 'status' e 'test-connection'
    """
    
    print(quickstart_text.strip())

@cli.command()
@click.option('--format', type=click.Choice(['table', 'json'], case_sensitive=False), 
              default='table', help='Formato output')
def list_jobs(format):
    """📋 Lista i job di importazione recenti."""
    try:
        importer = EnterpriseImporter()
        importer._setup_credentials()
        importer._setup_database_connection()
        
        # Query job recenti
        query = """
        SELECT job_id, name, status, created_at, processed_records, 
               valid_records, failed_records, integrity_verified
        FROM import_jobs 
        ORDER BY created_at DESC 
        LIMIT 10
        """
        
        result = importer.db_connection.execute_with_retry(query)
        
        if not result:
            print("📭 Nessun job trovato.")
            return
        
        if format == 'json':
            import json
            jobs = []
            for row in result:
                jobs.append({
                    'job_id': row[0],
                    'name': row[1],
                    'status': row[2],
                    'created_at': str(row[3]),
                    'processed_records': row[4],
                    'valid_records': row[5],
                    'failed_records': row[6],
                    'integrity_verified': bool(row[7])
                })
            print(json.dumps(jobs, indent=2))
        else:
            # Formato tabella
            print("📋 JOB DI IMPORTAZIONE RECENTI")
            print("=" * 80)
            print(f"{'ID':<8} {'Nome':<20} {'Stato':<12} {'Data':<19} {'Record':<8} {'✅':<6} {'❌':<6} {'🔒':<3}")
            print("-" * 80)
            
            for row in result:
                job_id_short = row[0][:8]
                name = row[1][:20] if row[1] else 'N/A'
                status = row[2]
                created_at = str(row[3])[:19] if row[3] else 'N/A'
                processed = row[4] or 0
                valid = row[5] or 0
                failed = row[6] or 0
                integrity = '✅' if row[7] else '❌'
                
                print(f"{job_id_short:<8} {name:<20} {status:<12} {created_at:<19} {processed:<8} {valid:<6} {failed:<6} {integrity:<3}")
        
    except Exception as e:
        print(f"❌ Errore nel recupero job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    cli()