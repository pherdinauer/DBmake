#!/usr/bin/env python3
"""
Sistema di Verifica dell'Integrità dei Dati
Verifica che tutti i dati JSON vengano correttamente inseriti nel database
"""

import os
import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
import mysql.connector
from mysql.connector import Error

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_integrity.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class FileIntegrityReport:
    """Report di integrità per un singolo file JSON"""
    filename: str
    source_records: int
    database_records: int
    missing_records: int
    success_rate: float
    processing_time: str
    file_size_mb: float
    file_hash: str
    timestamp: str
    errors: List[str]
    missing_data_details: List[Dict[str, Any]]

@dataclass 
class GlobalIntegrityReport:
    """Report globale di integrità per tutti i file elaborati"""
    total_files_processed: int
    total_source_records: int
    total_database_records: int
    total_missing_records: int
    global_success_rate: float
    processing_start: str
    processing_end: str
    files_with_issues: List[str]
    detailed_reports: List[FileIntegrityReport]

class DataIntegrityChecker:
    """
    Sistema principale per la verifica dell'integrità dei dati
    """
    
    def __init__(self, database_config: Dict[str, str]):
        self.db_config = database_config
        self.integrity_reports: List[FileIntegrityReport] = []
        self.missing_data_dir = Path("logs/missing_data")
        self.missing_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurazione dei percorsi dei dati JSON
        self.json_data_paths = {
            'aggiudicazioni': 'data/downloads/aggiudicazioni',
            'bandi-cig-tipo-scelta-contraente': 'data/downloads/bandi-cig-tipo-scelta-contraente',
            'bando-cig-modalita-realizzazione': 'data/downloads/bando-cig-modalita-realizzazione',
            'collaudo': 'data/downloads/collaudo',
            'fine-contratto': 'data/downloads/fine-contratto',
            'fonti-finanziamento': 'data/downloads/fonti-finanziamento',
            'lavorazioni': 'data/downloads/lavorazioni',
            'partecipanti': 'data/downloads/partecipanti',
            'pubblicazioni': 'data/downloads/pubblicazioni',
            'quadro-economico': 'data/downloads/quadro-economico',
            'sospensioni': 'data/downloads/sospensioni',
            'stati-avanzamento': 'data/downloads/stati-avanzamento',
            'subappalti': 'data/downloads/subappalti',
            'varianti': 'data/downloads/varianti'
        }
        
        # Mapping file JSON -> tabelle database
        self.table_mapping = {
            'aggiudicazioni': 'aggiudicazioni',
            'bandi-cig-tipo-scelta-contraente': 'bandi_cig_tipo_scelta_contraente',
            'bando-cig-modalita-realizzazione': 'bando_cig_modalita_realizzazione',
            'collaudo': 'collaudo',
            'fine-contratto': 'fine_contratto',
            'fonti-finanziamento': 'fonti_finanziamento',
            'lavorazioni': 'lavorazioni',
            'partecipanti': 'partecipanti',
            'pubblicazioni': 'pubblicazioni',
            'quadro-economico': 'quadro_economico',
            'sospensioni': 'sospensioni',
            'stati-avanzamento': 'stati_avanzamento',
            'subappalti': 'subappalti',
            'varianti': 'varianti'
        }
    
    def get_database_connection(self) -> mysql.connector.connection.MySQLConnection:
        """Crea connessione al database"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            return connection
        except Error as e:
            logger.error(f"❌ Errore connessione database: {e}")
            raise
    
    def calculate_file_hash(self, file_path: Path) -> str:
        """Calcola hash SHA256 del file per verifiche di integrità"""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception as e:
            logger.error(f"❌ Errore calcolo hash per {file_path}: {e}")
            return "unknown"
    
    def count_json_records(self, file_path: Path) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Conta i record in un file JSON e restituisce anche i dati per confronto
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Se è una lista di oggetti
            if isinstance(data, list):
                return len(data), data
            
            # Se è un oggetto con array
            elif isinstance(data, dict):
                # Cerca il primo array di dati
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        return len(value), value
                
                # Se non trova array, considera l'oggetto stesso
                return 1, [data]
            
            # Caso singolo oggetto
            else:
                return 1, [data]
                
        except Exception as e:
            logger.error(f"❌ Errore lettura JSON {file_path}: {e}")
            return 0, []
    
    def count_database_records(self, table_name: str) -> int:
        """
        Conta i record nel database per una specifica tabella
        """
        try:
            connection = self.get_database_connection()
            cursor = connection.cursor()
            
            query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(query)
                
            count = cursor.fetchone()[0]
            
            cursor.close()
            connection.close()
            
            return count
            
        except Error as e:
            logger.error(f"❌ Errore conteggio database per tabella {table_name}: {e}")
            return 0
    
    def find_missing_records(self, json_data: List[Dict[str, Any]], 
                           table_name: str, 
                           primary_key: str = "cig") -> List[Dict[str, Any]]:
        """
        Trova i record mancanti nel database confrontando con i dati JSON
        """
        missing_records = []
        
        try:
            connection = self.get_database_connection()
            cursor = connection.cursor()
            
            for record in json_data:
                if primary_key in record:
                    key_value = record[primary_key]
                    
                    # Verifica se il record esiste nel database
                    query = f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = %s"
                    cursor.execute(query, (key_value,))
                    
                    if cursor.fetchone()[0] == 0:
                        missing_records.append(record)
            
            cursor.close()
            connection.close()
            
        except Error as e:
            logger.error(f"❌ Errore ricerca record mancanti per {table_name}: {e}")
        
        return missing_records
    
    def save_missing_data_report(self, filename: str, missing_records: List[Dict[str, Any]]):
        """
        Salva un report dettagliato dei dati mancanti
        """
        if not missing_records:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        missing_file = self.missing_data_dir / f"missing_{filename}_{timestamp}.json"
        
        try:
            with open(missing_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'filename': filename,
                    'timestamp': timestamp,
                    'missing_count': len(missing_records),
                    'missing_records': missing_records
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"📄 Report dati mancanti salvato: {missing_file}")
            
        except Exception as e:
            logger.error(f"❌ Errore salvataggio report dati mancanti: {e}")
    
    def get_table_name_from_file(self, filename: str, dataset_type: str) -> str:
        """
        Determina il nome della tabella dal nome del file e tipo dataset
        """
        # Mapping base dai tipi di dataset
        base_mapping = {
            'aggiudicazioni': 'aggiudicazioni',
            'bandi-cig-tipo-scelta-contraente': 'bandi_cig_tipo_scelta_contraente',
            'bando-cig-modalita-realizzazione': 'bando_cig_modalita_realizzazione',
            'collaudo': 'collaudo',
            'fine-contratto': 'fine_contratto',
            'fonti-finanziamento': 'fonti_finanziamento',
            'lavorazioni': 'lavorazioni',
            'partecipanti': 'partecipanti',
            'pubblicazioni': 'pubblicazioni',
            'quadro-economico': 'quadro_economico',
            'sospensioni': 'sospensioni',
            'stati-avanzamento': 'stati_avanzamento',
            'subappalti': 'subappalti',
            'varianti': 'varianti'
        }
        
        return base_mapping.get(dataset_type, dataset_type.replace('-', '_'))
    
    def check_file_integrity(self, file_path: Path, dataset_type: str) -> FileIntegrityReport:
        """
        Verifica l'integrità di un singolo file JSON
        """
        start_time = datetime.now()
        logger.info(f"🔍 Verifica integrità: {file_path.name}")
        
        # Informazioni base del file
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        file_hash = self.calculate_file_hash(file_path)
        errors = []
        
        # Conta record nel JSON
        source_count, json_data = self.count_json_records(file_path)
        if source_count == 0:
            errors.append("Nessun record trovato nel file JSON")
        
        # Ottieni nome tabella corrispondente
        table_name = self.get_table_name_from_file(file_path.name, dataset_type)
        
        # Conta record nel database
        database_count = self.count_database_records(table_name)
        
        # Trova record mancanti solo se abbiamo dati JSON
        missing_records = []
        if json_data:
            missing_records = self.find_missing_records(json_data, table_name)
        
        missing_count = len(missing_records)
        
        # Calcola tasso di successo
        if source_count > 0:
            success_rate = ((source_count - missing_count) / source_count) * 100
        else:
            success_rate = 0.0
            errors.append("Impossibile calcolare tasso di successo: nessun record JSON")
        
        # Salva report dati mancanti se ce ne sono
        if missing_records:
            self.save_missing_data_report(file_path.name, missing_records)
        
        end_time = datetime.now()
        processing_time = str(end_time - start_time)
        
        # Crea report
        report = FileIntegrityReport(
            filename=file_path.name,
            source_records=source_count,
            database_records=database_count,
            missing_records=missing_count,
            success_rate=success_rate,
            processing_time=processing_time,
            file_size_mb=round(file_size_mb, 2),
            file_hash=file_hash,
            timestamp=start_time.isoformat(),
            errors=errors,
            missing_data_details=missing_records[:10]  # Prime 10 per non appesantire
        )
        
        # Log risultati
        if missing_count == 0:
            logger.info(f"✅ {file_path.name}: {source_count} record, 100% successo")
        else:
            logger.warning(f"⚠️ {file_path.name}: {missing_count}/{source_count} record mancanti ({success_rate:.1f}% successo)")
        
        if errors:
            for error in errors:
                logger.error(f"❌ {file_path.name}: {error}")
        
        return report
    
    def run_full_integrity_check(self) -> GlobalIntegrityReport:
        """
        Esegue verifica completa dell'integrità per tutti i dataset
        """
        start_time = datetime.now()
        logger.info("🚀 Avvio verifica completa integrità dati")
        
        all_reports = []
        files_with_issues = []
        
        for dataset_type, data_path in self.json_data_paths.items():
            data_dir = Path(data_path)
            
            if not data_dir.exists():
                logger.warning(f"⚠️ Directory non trovata: {data_dir}")
                continue
            
            # Trova tutti i file JSON nella directory
            json_files = list(data_dir.glob("*.json"))
            
            if not json_files:
                logger.warning(f"⚠️ Nessun file JSON trovato in: {data_dir}")
                continue
            
            logger.info(f"📂 Elaborazione {dataset_type}: {len(json_files)} file")
            
            for json_file in json_files:
                try:
                    report = self.check_file_integrity(json_file, dataset_type)
                    all_reports.append(report)
                    
                    if report.missing_records > 0 or report.errors:
                        files_with_issues.append(report.filename)
                        
                except Exception as e:
                    logger.error(f"❌ Errore elaborazione {json_file}: {e}")
                    files_with_issues.append(json_file.name)
        
        end_time = datetime.now()
        
        # Calcola statistiche globali
        total_source = sum(r.source_records for r in all_reports)
        total_database = sum(r.database_records for r in all_reports) 
        total_missing = sum(r.missing_records for r in all_reports)
        
        global_success_rate = 0.0
        if total_source > 0:
            global_success_rate = ((total_source - total_missing) / total_source) * 100
        
        # Crea report globale
        global_report = GlobalIntegrityReport(
            total_files_processed=len(all_reports),
            total_source_records=total_source,
            total_database_records=total_database,
            total_missing_records=total_missing,
            global_success_rate=global_success_rate,
            processing_start=start_time.isoformat(),
            processing_end=end_time.isoformat(),
            files_with_issues=files_with_issues,
            detailed_reports=all_reports
        )
        
        self.save_global_report(global_report)
        self.print_summary(global_report)
        
        return global_report
    
    def save_global_report(self, report: GlobalIntegrityReport):
        """
        Salva il report globale in formato JSON
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = Path(f"logs/integrity_report_{timestamp}.json")
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(report), f, indent=2, ensure_ascii=False)
            
            logger.info(f"📊 Report globale salvato: {report_file}")
            
        except Exception as e:
            logger.error(f"❌ Errore salvataggio report globale: {e}")
    
    def print_summary(self, report: GlobalIntegrityReport):
        """
        Stampa un riepilogo dettagliato dei risultati
        """
        print("\n" + "="*80)
        print("📊 RIEPILOGO VERIFICA INTEGRITÀ DATI")
        print("="*80)
        
        print(f"📁 File elaborati: {report.total_files_processed}")
        print(f"📄 Record JSON totali: {report.total_source_records:,}")
        print(f"🗄️ Record database totali: {report.total_database_records:,}")
        print(f"❌ Record mancanti: {report.total_missing_records:,}")
        print(f"✅ Tasso successo globale: {report.global_success_rate:.2f}%")
        
        if report.files_with_issues:
            print(f"\n⚠️ File con problemi ({len(report.files_with_issues)}):")
            for filename in report.files_with_issues:
                print(f"   • {filename}")
        
        print(f"\n⏱️ Durata elaborazione: {(datetime.fromisoformat(report.processing_end) - datetime.fromisoformat(report.processing_start)).total_seconds():.1f} secondi")
        
        # Top 5 file con più problemi
        problematic_files = [r for r in report.detailed_reports if r.missing_records > 0]
        if problematic_files:
            top_issues = sorted(problematic_files, key=lambda x: x.missing_records, reverse=True)[:5]
            print(f"\n🔥 Top 5 file con più record mancanti:")
            for i, file_report in enumerate(top_issues, 1):
                print(f"   {i}. {file_report.filename}: {file_report.missing_records:,} mancanti "
                      f"({file_report.success_rate:.1f}% successo)")
        
        print("\n" + "="*80)
    
    def generate_detailed_log(self, report: GlobalIntegrityReport):
        """
        Genera un log dettagliato per ogni file elaborato
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = Path(f"logs/detailed_integrity_log_{timestamp}.txt")
        
        try:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write("VERIFICA DETTAGLIATA INTEGRITÀ DATI\n")
                f.write("="*50 + "\n\n")
                
                for file_report in report.detailed_reports:
                    f.write(f"File: {file_report.filename}\n")
                    f.write(f"Record JSON: {file_report.source_records:,}\n")
                    f.write(f"Record DB: {file_report.database_records:,}\n")
                    f.write(f"Mancanti: {file_report.missing_records:,}\n")
                    f.write(f"Successo: {file_report.success_rate:.2f}%\n")
                    f.write(f"Dimensione: {file_report.file_size_mb} MB\n")
                    f.write(f"Hash: {file_report.file_hash}\n")
                    f.write(f"Tempo: {file_report.processing_time}\n")
                    if file_report.errors:
                        f.write(f"Errori: {', '.join(file_report.errors)}\n")
                    f.write("-" * 30 + "\n\n")
            
            logger.info(f"📝 Log dettagliato salvato: {log_file}")
            
        except Exception as e:
            logger.error(f"❌ Errore generazione log dettagliato: {e}")

def main():
    """Funzione principale per eseguire la verifica dell'integrità"""
    
    # Assicurati che le directory dei log esistano
    Path("logs").mkdir(exist_ok=True)
    Path("logs/missing_data").mkdir(exist_ok=True)
    
    # Configurazione database (puoi modificare secondo le tue impostazioni)
    db_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'user': os.getenv('MYSQL_USER', 'Nando'),
        'password': os.getenv('MYSQL_PASSWORD', ''),
        'database': os.getenv('MYSQL_DATABASE', 'anac_import3'),
        'charset': 'utf8mb4',
        'collation': 'utf8mb4_unicode_ci'
    }
    
    try:
        # Crea checker
        checker = DataIntegrityChecker(db_config)
        
        # Esegui verifica completa
        global_report = checker.run_full_integrity_check()
        
        # Genera log dettagliato
        checker.generate_detailed_log(global_report)
        
        # Ritorna codice di uscita appropriato
        if global_report.total_missing_records == 0:
            logger.info("🎉 Verifica completata: nessun dato mancante!")
            return 0
        else:
            logger.warning(f"⚠️ Verifica completata: {global_report.total_missing_records} record mancanti")
            return 1
            
    except Exception as e:
        logger.error(f"💥 Errore fatale durante verifica integrità: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 2

if __name__ == "__main__":
    exit(main()) 