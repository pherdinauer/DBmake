from pathlib import Path
import os
from typing import List

# Base paths
BASE_PATH = Path("/FileSystem/anacd2/downloads")  # Percorso fisso per i file JSON
DB_PATH = Path("/database/anac.db")  # Mount point /database su /dev/sdc3
BACKUP_PATH = Path("/database/backups")  # Backup nella stessa partizione del DB

# Database settings
DB_SETTINGS = {
    "timeout": 30,
    "isolation_level": None,  # Autocommit mode
    "check_same_thread": False
}

# Logging settings
LOG_PATH = Path("logs")
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Cartelle da importare
CARTELLE_RILEVANTI: List[str] = [
    "bando-cig-modalita-realizzazione",
    "bandi-cig-tipo-scelta-contraente",
    "aggiudicazioni",
    "partecipanti",
    "collaudo",
    "fine-contratto",
    "lavorazioni",
    "pubblicazioni",
    "quadro-economico",
    "sospensioni",
    "stati-avanzamento",
    "subappalti",
    "varianti",
    "fonti-finanziamento"
]

# Validazione dati
REQUIRED_COLUMNS = {
    "aggiudicazioni": ["CIG", "ID_AGGIUDICAZIONE"],
    "partecipanti": ["CIG", "ID_PARTECIPANTE"],
    # Aggiungere altre tabelle e colonne richieste
}

# Backup settings
BACKUP_RETENTION_DAYS = 7 