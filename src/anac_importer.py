import os
import sqlite3
import pandas as pd
import json
from pathlib import Path
import logging
from datetime import datetime
import shutil
from typing import List, Dict, Any, Optional
import sys
import subprocess

# Import configurazione
sys.path.append(str(Path(__file__).parent.parent))
from config.config import (
    BASE_PATH, DB_PATH, BACKUP_PATH, DB_SETTINGS,
    LOG_PATH, LOG_LEVEL, LOG_FORMAT,
    CARTELLE_RILEVANTI, REQUIRED_COLUMNS,
    BACKUP_RETENTION_DAYS
)

# Setup logging
LOG_PATH.mkdir(exist_ok=True)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_PATH / f"anac_import_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AnacImporter:
    def __init__(self):
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self._verify_base_path()
        self._setup_database()
        self._setup_backup()

    def _verify_base_path(self) -> None:
        """Verifica l'accesso al percorso base e alle cartelle."""
        logger.info(f"🔍 Verifica percorso base: {BASE_PATH}")
        
        # Verifica se il percorso esiste
        if not BASE_PATH.exists():
            logger.error(f"❌ Il percorso base {BASE_PATH} non esiste!")
            # Prova a listare il contenuto della directory padre
            parent = BASE_PATH.parent
            if parent.exists():
                logger.info(f"📂 Contenuto di {parent}:")
                try:
                    for item in parent.iterdir():
                        logger.info(f"  - {item.name} ({'directory' if item.is_dir() else 'file'})")
                except Exception as e:
                    logger.error(f"❌ Errore nel listare {parent}: {e}")
            return

        # Verifica i permessi
        if not os.access(BASE_PATH, os.R_OK):
            logger.error(f"❌ Non hai i permessi di lettura per {BASE_PATH}")
            # Mostra i permessi attuali
            try:
                if os.name == 'nt':  # Windows
                    perms = subprocess.check_output(['icacls', str(BASE_PATH)]).decode()
                else:  # Linux/Unix
                    perms = subprocess.check_output(['ls', '-la', str(BASE_PATH)]).decode()
                logger.info(f"📋 Permessi attuali:\n{perms}")
            except Exception as e:
                logger.error(f"❌ Errore nel verificare i permessi: {e}")
            return

        logger.info(f"✅ Percorso base verificato: {BASE_PATH}")
        
        # Lista il contenuto della directory base
        logger.info(f"📂 Contenuto di {BASE_PATH}:")
        try:
            for item in BASE_PATH.iterdir():
                logger.info(f"  - {item.name} ({'directory' if item.is_dir() else 'file'})")
        except Exception as e:
            logger.error(f"❌ Errore nel listare {BASE_PATH}: {e}")
        
        # Verifica ogni cartella
        for cartella in CARTELLE_RILEVANTI:
            path_cartella = BASE_PATH / cartella
            logger.info(f"🔍 Verifica cartella: {cartella}")
            
            if not path_cartella.exists():
                logger.warning(f"⚠️ Cartella non trovata: {cartella}")
                continue
                
            if not os.access(path_cartella, os.R_OK):
                logger.error(f"❌ Non hai i permessi di lettura per {cartella}")
                continue
                
            # Cerca i file JSON in modo ricorsivo
            json_files = list(path_cartella.rglob("*.json"))
            logger.info(f"📁 Cartella {cartella}: {len(json_files)} file JSON trovati")
            
            # Lista i primi 5 file JSON se presenti
            if json_files:
                logger.info(f"📄 Primi 5 file in {cartella}:")
                for file in json_files[:5]:
                    logger.info(f"  - {file.relative_to(path_cartella)}")
                    
                # Verifica il contenuto del primo file
                if json_files:
                    try:
                        with open(json_files[0], 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            logger.info(f"📋 Struttura del primo file ({json_files[0].name}):")
                            if isinstance(data, list):
                                logger.info(f"  - Tipo: Lista con {len(data)} elementi")
                                if data:
                                    logger.info(f"  - Primo elemento: {list(data[0].keys())}")
                            elif isinstance(data, dict):
                                logger.info(f"  - Tipo: Dizionario con chiavi: {list(data.keys())}")
                    except Exception as e:
                        logger.error(f"❌ Errore nella lettura del file {json_files[0]}: {e}")

    def _setup_database(self) -> None:
        """Inizializza la connessione al database."""
        try:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(DB_PATH, **DB_SETTINGS)
            self.cursor = self.conn.cursor()
            logger.info(f"Connessione al database stabilita: {DB_PATH}")
        except Exception as e:
            logger.error(f"Errore nella connessione al database: {e}")
            raise

    def _setup_backup(self) -> None:
        """Prepara la directory per i backup."""
        BACKUP_PATH.mkdir(parents=True, exist_ok=True)

    def _create_backup(self) -> None:
        """Crea un backup del database."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = BACKUP_PATH / f"anac_backup_{timestamp}.db"
        
        try:
            shutil.copy2(DB_PATH, backup_file)
            logger.info(f"Backup creato: {backup_file}")
            
            # Rimuovi backup vecchi
            self._cleanup_old_backups()
        except Exception as e:
            logger.error(f"Errore nella creazione del backup: {e}")

    def _cleanup_old_backups(self) -> None:
        """Rimuove i backup più vecchi di BACKUP_RETENTION_DAYS."""
        try:
            current_time = datetime.now()
            for backup_file in BACKUP_PATH.glob("anac_backup_*.db"):
                file_time = datetime.fromtimestamp(backup_file.stat().st_mtime)
                if (current_time - file_time).days > BACKUP_RETENTION_DAYS:
                    backup_file.unlink()
                    logger.info(f"Rimosso backup vecchio: {backup_file}")
        except Exception as e:
            logger.error(f"Errore nella pulizia dei backup: {e}")

    def _validate_dataframe(self, df: pd.DataFrame, table_name: str) -> bool:
        """Valida il DataFrame rispetto ai requisiti della tabella."""
        if table_name in REQUIRED_COLUMNS:
            required = REQUIRED_COLUMNS[table_name]
            missing = [col for col in required if col not in df.columns]
            if missing:
                logger.error(f"Colonne richieste mancanti in {table_name}: {missing}")
                return False
        return True

    def importa_cartella_json(self, cartella: str) -> None:
        """Importa i file JSON da una cartella nel database."""
        path_cartella = BASE_PATH / cartella
        if not path_cartella.is_dir():
            logger.error(f"Cartella non trovata: {cartella}")
            return

        all_records = []
        nome_tabella = cartella.replace("-", "_")
        json_files = list(path_cartella.rglob("*.json"))
        
        if not json_files:
            logger.warning(f"⚠️ Nessun file JSON trovato in {cartella}")
            return

        logger.info(f"📂 Elaborazione {len(json_files)} file in {cartella}")

        for file in json_files:
            try:
                records = []
                with open(file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:  # Skip empty lines
                            try:
                                data = json.loads(line)
                                records.append(data)
                            except json.JSONDecodeError as e:
                                logger.error(f"Errore nel parsing della linea in {file}: {e}")
                                continue
                    
                    if records:
                        df = pd.json_normalize(records)
                        df["__origine_file__"] = str(file.relative_to(path_cartella))
                        all_records.append(df)
                        logger.debug(f"Processato file: {file.relative_to(path_cartella)}")

            except Exception as e:
                logger.error(f"Errore nel file {file.relative_to(path_cartella)}: {e}")

        if all_records:
            try:
                df_all = pd.concat(all_records, ignore_index=True)
                
                # Validazione dati
                if not self._validate_dataframe(df_all, nome_tabella):
                    logger.error(f"Validazione fallita per la tabella {nome_tabella}")
                    return

                # Salvataggio nel database
                df_all.to_sql(nome_tabella, self.conn, if_exists='replace', index=False)
                logger.info(f"✅ Importata tabella '{nome_tabella}' con {len(df_all)} righe")
                
            except Exception as e:
                logger.error(f"Errore nell'importazione della tabella {nome_tabella}: {e}")

    def importa_tutto(self) -> None:
        """Importa tutte le cartelle configurate."""
        try:
            self._create_backup()
            
            for cartella in CARTELLE_RILEVANTI:
                logger.info(f"Importazione cartella: {cartella}")
                self.importa_cartella_json(cartella)
                
            logger.info("Importazione completata con successo")
            
        except Exception as e:
            logger.error(f"Errore durante l'importazione: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Connessione al database chiusa")

if __name__ == "__main__":
    importer = AnacImporter()
    importer.importa_tutto() 