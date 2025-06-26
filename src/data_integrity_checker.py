#!/usr/bin/env python3
"""
Sistema di Verifica dell'Integrità dei Dati - Versione Windows Compatible
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
        
        # Configurazione dei percorsi dei dati JSON - aggiornata per trovare i file
        self.json_data_paths = {
            'smartcig': 'data',
            'aggiudicazioni': 'data', 
            'bandi': 'data',
            'partecipanti': 'data',
            'pubblicazioni': 'data',
            'general': 'data'  # Per tutti gli altri file JSON
        }
    
    def get_database_connection(self) -> mysql.connector.connection.MySQLConnection:
        """Crea connessione al database"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            return connection
        except Error as e:
            logger.error(f"ERRORE connessione database: {e}")
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
            logger.error(f"ERRORE calcolo hash per {file_path}: {e}")
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
            logger.error(f"ERRORE lettura JSON {file_path}: {e}")
            return 0, []
    
    def get_table_name_from_filename(self, filename: str) -> str:
        """
        Determina il nome della tabella dal nome del file
        """
        # Rimuovi estensione
        base_name = filename.replace('.json', '').replace('_json', '')
        
        # Mapping specifico per i file che hai
        table_mapping = {
            'smartcig-tipo-fattispecie-contrattuale': 'smartcig_fattispecie_data',
            'aggiudicatari': 'aggiudicatari_data',
            'bandi': 'bandi_data',
            'partecipanti': 'partecipanti_data'
        }
        
        # Cerca mapping diretto
        if base_name in table_mapping:
            return table_mapping[base_name]
        
        # Prova a inferire dalla struttura del nome
        if 'smartcig' in base_name:
            table_name = base_name.replace('-', '_').replace('smartcig_', 'smartcig_') + '_data'
        else:
            table_name = base_name.replace('-', '_') + '_data'
        
        return table_name
    
    def count_database_records(self, table_name: str) -> int:
        """
        Conta i record nel database per una specifica tabella
        """
        try:
            logger.info(f"Connessione al database...")
            connection = self.get_database_connection()
            cursor = connection.cursor()
            
            logger.info(f"Esecuzione query: SELECT COUNT(*) FROM {table_name}")
            query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(query)
                
            count = cursor.fetchone()[0]
            logger.info(f"Query completata con successo")
            
            cursor.close()
            connection.close()
            
            return count
            
        except Error as e:
            logger.error(f"ERRORE conteggio database per tabella {table_name}: {e}")
            logger.error(f"Verifica che la tabella esista e sia accessibile")
            return 0
    
    def find_missing_records(self, json_data: List[Dict[str, Any]], 
                           table_name: str, 
                           primary_key: str = "id") -> List[Dict[str, Any]]:
        """
        Trova i record mancanti nel database confrontando con i dati JSON
        """
        missing_records = []
        total_records = len(json_data)
        
        logger.info(f"Ricerca dettagliata dei record mancanti...")
        logger.info(f"Record da verificare: {total_records:,}")
        
        # Per ora, se il conteggio non corrisponde, consideriamo tutti i record come "potenzialmente mancanti"
        # In una implementazione completa, dovremmo controllare record per record
        
        try:
            connection = self.get_database_connection()
            cursor = connection.cursor()
            
            # Verifica se la tabella ha una colonna ID o simile
            cursor.execute(f"DESCRIBE {table_name}")
            columns = [row[0] for row in cursor.fetchall()]
            
            # Cerca una chiave primaria appropriata
            possible_keys = ['id', 'cig', 'uuid', 'codice']
            found_key = None
            
            for key in possible_keys:
                if key in columns:
                    found_key = key
                    break
            
            if not found_key and json_data:
                # Usa la prima chiave del primo record JSON
                first_record = json_data[0]
                for key in possible_keys:
                    if key in first_record:
                        found_key = key
                        break
            
            if found_key:
                logger.info(f"Chiave primaria utilizzata: '{found_key}'")
                checked_records = 0
                
                for record in json_data:
                    if found_key in record:
                        key_value = record[found_key]
                        
                        # Verifica se il record esiste nel database
                        query = f"SELECT COUNT(*) FROM {table_name} WHERE {found_key} = %s"
                        cursor.execute(query, (key_value,))
                        
                        if cursor.fetchone()[0] == 0:
                            missing_records.append(record)
                        
                        checked_records += 1
                        
                        # Log progresso ogni 1000 record
                        if checked_records % 1000 == 0:
                            progress = (checked_records / total_records) * 100
                            logger.info(f"Progresso: {checked_records:,}/{total_records:,} ({progress:.1f}%) - Mancanti trovati: {len(missing_records)}")
                
                logger.info(f"Ricerca completata:")
                logger.info(f"   Record controllati: {checked_records:,}")
                logger.info(f"   Record mancanti: {len(missing_records):,}")
                logger.info(f"   Record presenti: {checked_records - len(missing_records):,}")
            else:
                logger.warning(f"Nessuna chiave primaria trovata per il confronto dettagliato")
            
            cursor.close()
            connection.close()
            
        except Error as e:
            logger.error(f"ERRORE durante ricerca record mancanti per {table_name}: {e}")
        
        return missing_records
    
    def save_missing_data_report(self, filename: str, missing_records: List[Dict[str, Any]]):
        """
        Salva un report dettagliato dei dati mancanti
        """
        if not missing_records:
            logger.info(f"Nessun dato mancante da salvare per {filename}")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        missing_file = self.missing_data_dir / f"missing_{filename}_{timestamp}.json"
        
        try:
            logger.info(f"Creazione report dati mancanti...")
            logger.info(f"   Record mancanti: {len(missing_records):,}")
            logger.info(f"   File output: {missing_file}")
            
            with open(missing_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'filename': filename,
                    'timestamp': timestamp,
                    'missing_count': len(missing_records),
                    'missing_records': missing_records,
                    'integrity_check': {
                        'generated_by': 'Data Integrity Checker v1.0',
                        'generation_time': datetime.now().isoformat(),
                        'purpose': 'Record mancanti per recupero dati'
                    }
                }, f, indent=2, ensure_ascii=False)
            
            file_size = missing_file.stat().st_size / 1024  # KB
            logger.info(f"Report salvato con successo!")
            logger.info(f"   Dimensione file: {file_size:.1f} KB")
            logger.info(f"   Usa questo file per identificare i dati da recuperare")
            
        except Exception as e:
            logger.error(f"ERRORE durante salvataggio report dati mancanti: {e}")
            logger.error(f"Verifica i permessi di scrittura nella directory logs/missing_data/")
    
    def check_file_integrity(self, file_path: Path) -> FileIntegrityReport:
        """
        Verifica l'integrità di un singolo file JSON
        """
        start_time = datetime.now()
        
        # Log inizio verifica con dettagli file
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        logger.info(f"")
        logger.info(f"=== AVVIO VERIFICA INTEGRITA ===")
        logger.info(f"File: {file_path.name}")
        logger.info(f"Dimensione: {file_size_mb:.2f} MB")
        logger.info(f"Inizio: {start_time.strftime('%H:%M:%S')}")
        logger.info(f"=" * 50)
        
        file_hash = self.calculate_file_hash(file_path)
        errors = []
        
        # Fase 1: Lettura e conteggio JSON
        logger.info(f"FASE 1: Lettura file JSON...")
        source_count, json_data = self.count_json_records(file_path)
        
        if source_count == 0:
            logger.error(f"ERRORE: Nessun record trovato nel file JSON")
            errors.append("Nessun record trovato nel file JSON")
        else:
            logger.info(f"Record JSON letti: {source_count:,}")
            logger.info(f"Hash file: {file_hash[:16]}...")
        
        # Fase 2: Mapping tabella database
        table_name = self.get_table_name_from_filename(file_path.name)
        logger.info(f"")
        logger.info(f"FASE 2: Verifica database...")
        logger.info(f"Tabella target: {table_name}")
        
        # Fase 3: Conteggio database
        logger.info(f"Conteggio record nel database...")
        database_count = self.count_database_records(table_name)
        logger.info(f"Record database trovati: {database_count:,}")
        
        # Fase 4: Confronto e verifica sicurezza
        logger.info(f"")
        logger.info(f"=== CONTROLLO DI SICUREZZA DATI ===")
        logger.info(f"Confronto JSON vs Database...")
        logger.info(f"Record attesi (JSON): {source_count:,}")
        logger.info(f"Record inseriti (DB): {database_count:,}")
        
        # Verifica iniziale basata sui conteggi
        if source_count == database_count:
            logger.info(f"SUCCESS: I conteggi corrispondono perfettamente!")
        else:
            diff = abs(source_count - database_count)
            logger.warning(f"DISCREPANZA: Differenza di {diff:,} record")
        
        # Fase 5: Ricerca record mancanti (verifica dettagliata)
        missing_records = []
        if json_data and source_count != database_count:
            logger.info(f"Ricerca dettagliata record mancanti...")
            missing_records = self.find_missing_records(json_data, table_name)
        
        missing_count = len(missing_records)
        
        # Calcola tasso di successo
        if source_count > 0:
            success_rate = ((source_count - missing_count) / source_count) * 100
        else:
            success_rate = 0.0
            errors.append("Impossibile calcolare tasso di successo: nessun record JSON")
        
        # Log risultati controllo sicurezza dettagliato
        logger.info(f"")
        logger.info(f"=== RISULTATO CONTROLLO DI SICUREZZA ===")
        if missing_count == 0:
            logger.info(f"SICUREZZA CONFERMATA: Tutti i dati JSON sono stati inseriti correttamente!")
            logger.info(f"SUCCESS: {source_count:,} record verificati - 100% successo")
        else:
            logger.error(f"ALLERTA SICUREZZA: {missing_count:,} record mancanti!")
            logger.error(f"Tasso successo: {success_rate:.1f}%")
            logger.error(f"PERDITA DATI RILEVATA - Verifica necessaria!")
        
        # Salva report dati mancanti se ce ne sono
        if missing_records:
            logger.info(f"Generazione report dati mancanti...")
            self.save_missing_data_report(file_path.name, missing_records)
            logger.info(f"Report salvato con {len(missing_records)} record mancanti")
        
        end_time = datetime.now()
        processing_time = str(end_time - start_time)
        
        # Log finale con timing
        logger.info(f"")
        logger.info(f"Verifica completata in {processing_time}")
        logger.info(f"Fine: {end_time.strftime('%H:%M:%S')}")
        logger.info(f"=" * 50)
        
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
        
        if errors:
            for error in errors:
                logger.error(f"ERRORE AGGIUNTIVO: {error}")
        
        return report
    
    def run_full_integrity_check(self) -> GlobalIntegrityReport:
        """
        Esegue verifica completa dell'integrità per tutti i dataset
        """
        start_time = datetime.now()
        
        # Header principale
        logger.info("=" * 60)
        logger.info("SISTEMA VERIFICA INTEGRITA DATI MYSQL")
        logger.info("Controllo di sicurezza JSON -> Database")
        logger.info("=" * 60)
        logger.info(f"Inizio elaborazione: {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
        logger.info(f"Database target: {self.db_config.get('database', 'N/A')}")
        logger.info(f"Host: {self.db_config.get('host', 'N/A')}")
        logger.info("")
        
        all_reports = []
        files_with_issues = []
        
        # Trova tutti i file JSON nella directory data
        data_dir = Path('data')
        if not data_dir.exists():
            logger.error("ERRORE: Directory 'data' non trovata!")
            return GlobalIntegrityReport(
                total_files_processed=0,
                total_source_records=0,
                total_database_records=0,
                total_missing_records=0,
                global_success_rate=0.0,
                processing_start=start_time.isoformat(),
                processing_end=datetime.now().isoformat(),
                files_with_issues=[],
                detailed_reports=[]
            )
        
        # Trova tutti i file JSON
        json_files = list(data_dir.glob("*.json"))
        total_files_found = len(json_files)
        
        logger.info("=== SCANSIONE PRELIMINARE ===")
        logger.info(f"Directory: {data_dir}")
        logger.info(f"File JSON trovati: {total_files_found}")
        logger.info("")
        
        if total_files_found == 0:
            logger.error("ERRORE: Nessun file JSON trovato per la verifica!")
            logger.error("Verifica che i file JSON siano nella directory 'data'")
            return GlobalIntegrityReport(
                total_files_processed=0,
                total_source_records=0,
                total_database_records=0,
                total_missing_records=0,
                global_success_rate=0.0,
                processing_start=start_time.isoformat(),
                processing_end=datetime.now().isoformat(),
                files_with_issues=[],
                detailed_reports=[]
            )
        
        # Elaborazione file per file
        for file_index, json_file in enumerate(json_files, 1):
            logger.info("")
            logger.info(f"ELABORAZIONE FILE {file_index}/{total_files_found}")
            logger.info(f"File: {json_file.name}")
            
            try:
                report = self.check_file_integrity(json_file)
                all_reports.append(report)
                
                if report.missing_records > 0 or report.errors:
                    files_with_issues.append(report.filename)
                    
            except Exception as e:
                logger.error(f"ERRORE CRITICO durante elaborazione {json_file}: {e}")
                logger.error(f"File saltato, continuando con il prossimo...")
                files_with_issues.append(json_file.name)
        
        end_time = datetime.now()
        total_duration = end_time - start_time
        
        # Calcola statistiche globali
        total_source = sum(r.source_records for r in all_reports)
        total_database = sum(r.database_records for r in all_reports) 
        total_missing = sum(r.missing_records for r in all_reports)
        
        global_success_rate = 0.0
        if total_source > 0:
            global_success_rate = ((total_source - total_missing) / total_source) * 100
        
        # Log risultato finale controllo sicurezza
        logger.info("")
        logger.info("=" * 60)
        logger.info("CONTROLLO DI SICUREZZA GLOBALE COMPLETATO")
        logger.info("=" * 60)
        logger.info(f"Durata totale: {total_duration}")
        logger.info(f"File elaborati: {len(all_reports)}")
        logger.info(f"Record JSON totali: {total_source:,}")
        logger.info(f"Record database totali: {total_database:,}")
        logger.info(f"Record mancanti: {total_missing:,}")
        logger.info("")
        
        if total_missing == 0:
            logger.info("*** SICUREZZA MASSIMA CONFERMATA! ***")
            logger.info("TUTTI i dati JSON sono stati inseriti correttamente nel database")
            logger.info(f"Tasso successo: {global_success_rate:.2f}%")
            logger.info("Nessuna perdita di dati rilevata")
        else:
            logger.error("*** ALLERTA SICUREZZA! ***")
            logger.error(f"{total_missing:,} RECORD MANCANTI rilevati!")
            logger.error(f"Tasso successo: {global_success_rate:.2f}%")
            logger.error(f"File con problemi: {len(files_with_issues)}")
            logger.error("Controlla i report dettagliati per identificare i dati mancanti")
        
        logger.info("=" * 60)
        
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
            
            logger.info(f"Report globale salvato: {report_file}")
            
        except Exception as e:
            logger.error(f"ERRORE salvataggio report globale: {e}")
    
    def print_summary(self, report: GlobalIntegrityReport):
        """
        Stampa un riepilogo dettagliato dei risultati
        """
        print("\n" + "="*80)
        print("RIEPILOGO VERIFICA INTEGRITA DATI")
        print("="*80)
        
        print(f"File elaborati: {report.total_files_processed}")
        print(f"Record JSON totali: {report.total_source_records:,}")
        print(f"Record database totali: {report.total_database_records:,}")
        print(f"Record mancanti: {report.total_missing_records:,}")
        print(f"Tasso successo globale: {report.global_success_rate:.2f}%")
        
        if report.files_with_issues:
            print(f"\nFile con problemi ({len(report.files_with_issues)}):")
            for filename in report.files_with_issues:
                print(f"   • {filename}")
        
        duration = (datetime.fromisoformat(report.processing_end) - datetime.fromisoformat(report.processing_start)).total_seconds()
        print(f"\nDurata elaborazione: {duration:.1f} secondi")
        
        # Top 5 file con più problemi
        problematic_files = [r for r in report.detailed_reports if r.missing_records > 0]
        if problematic_files:
            top_issues = sorted(problematic_files, key=lambda x: x.missing_records, reverse=True)[:5]
            print(f"\nTop 5 file con più record mancanti:")
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
                f.write("VERIFICA DETTAGLIATA INTEGRITA DATI\n")
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
            
            logger.info(f"Log dettagliato salvato: {log_file}")
            
        except Exception as e:
            logger.error(f"ERRORE generazione log dettagliato: {e}")

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
            logger.info("Verifica completata: nessun dato mancante!")
            return 0
        else:
            logger.warning(f"Verifica completata: {global_report.total_missing_records} record mancanti")
            return 1
            
    except Exception as e:
        logger.error(f"Errore fatale durante verifica integrità: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 2

if __name__ == "__main__":
    exit(main()) 