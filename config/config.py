from pathlib import Path
import os
from typing import List

# Base paths
WORKSPACE_PATH = Path(__file__).parent.parent
BASE_PATH = Path("/database/JSON")  # Percorso base dei file JSON
DB_PATH = WORKSPACE_PATH / "database" / "anac.db"  # Database SQLite
BACKUP_PATH = WORKSPACE_PATH / "database" / "backups"  # Backup nella stessa partizione del DB

# Verifica che il percorso base esista
if not BASE_PATH.exists():
    print(f"⚠️ ATTENZIONE: Il percorso {BASE_PATH} non esiste!")
    print("Per favore, verifica il percorso corretto dei file JSON e aggiorna BASE_PATH in config/config.py")
    print("Esempio di percorso corretto: /database/JSON")

# Database settings
DB_SETTINGS = {
    "timeout": 30,
    "isolation_level": None,  # Autocommit mode
    "check_same_thread": False
}

# Logging settings
LOG_PATH = WORKSPACE_PATH / "logs"
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