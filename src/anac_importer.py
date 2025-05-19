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
        self._setup_database()
        self._setup_backup()

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

        for file in path_cartella.glob("*.json"):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    
                    # Normalizzazione dati
                    if isinstance(data, list):
                        df = pd.json_normalize(data)
                    elif isinstance(data, dict):
                        liste = [v for v in data.values() if isinstance(v, list)]
                        if liste:
                            df = pd.json_normalize(liste[0])
                        else:
                            df = pd.json_normalize(data)
                    else:
                        continue

                    df["__origine_file__"] = str(file.name)
                    all_records.append(df)
                    logger.debug(f"Processato file: {file.name}")

            except Exception as e:
                logger.error(f"Errore nel file {file.name}: {e}")

        if all_records:
            try:
                df_all = pd.concat(all_records, ignore_index=True)
                
                # Validazione dati
                if not self._validate_dataframe(df_all, nome_tabella):
                    logger.error(f"Validazione fallita per la tabella {nome_tabella}")
                    return

                # Salvataggio nel database
                df_all.to_sql(nome_tabella, self.conn, if_exists='replace', index=False)
                logger.info(f"Importata tabella '{nome_tabella}' con {len(df_all)} righe")
                
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