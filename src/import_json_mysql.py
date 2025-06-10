import os
import json
import time
import gc
import glob
import mysql.connector
from mysql.connector import errorcode
import psutil
import threading
import math
from dotenv import load_dotenv
from collections import defaultdict
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import multiprocessing
import platform
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Union, Set, DefaultDict

# Import dei moduli separati
try:
    # Prova import assolute (quando eseguito come script)
    from src.database import DatabaseManager
    from src.utils import (
        LogContext, log_memory_status, log_performance_stats, 
        log_file_progress, log_batch_progress, log_error_with_context,
        check_disk_space
    )
except ImportError:
    # Fallback su import relative (quando eseguito come modulo)
    from .database import DatabaseManager
    from .utils import (
        LogContext, log_memory_status, log_performance_stats, 
        log_file_progress, log_batch_progress, log_error_with_context,
        check_disk_space
    )

# Crea la directory dei log se non esiste prima della configurazione logging
logs_dir = Path('logs')
logs_dir.mkdir(exist_ok=True)

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Logger specializzati per diversi componenti
analysis_logger = logger.getChild('analysis')
import_logger = logger.getChild('import')
batch_logger = logger.getChild('batch')
memory_logger = logger.getChild('memory')
db_logger = logger.getChild('database')
progress_logger = logger.getChild('progress')

# Carica le variabili d'ambiente
load_dotenv()

# Parametri di connessione da variabili d'ambiente
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_USER = os.environ.get('MYSQL_USER', 'Nando')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'DataBase2025!')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'anac_import3')
JSON_BASE_PATH = os.environ.get('ANAC_BASE_PATH', '/database/JSON')  # Ripristinato percorso originale
BATCH_SIZE = int(os.environ.get('IMPORT_BATCH_SIZE', 75000))  # Aumentato da 25k a 75k

# Configurazione dinamica delle risorse
import multiprocessing
import psutil

# Configurazioni sistema
CPU_CORES = multiprocessing.cpu_count()
TOTAL_RAM_GB = psutil.virtual_memory().total / (1024**3)

# RILEVAMENTO AUTOMATICO HIGH-PERFORMANCE
def detect_high_performance_capability():
    """Rileva automaticamente se il sistema può gestire la modalità high-performance"""
    cpu_capable = CPU_CORES >= 8  # Almeno 8 core
    ram_capable = TOTAL_RAM_GB >= 12  # Almeno 12GB RAM
    load_capable = True  # Assumiamo sempre ok per ora
    
    if cpu_capable and ram_capable:
        logger.info(f"🚀 AUTO-DETECT: Sistema potente rilevato!")
        logger.info(f"   CPU: {CPU_CORES} core ✅")
        logger.info(f"   RAM: {TOTAL_RAM_GB:.1f}GB ✅")
        return True
    else:
        logger.info(f"🏃 AUTO-DETECT: Sistema standard rilevato")
        logger.info(f"   CPU: {CPU_CORES} core {'✅' if cpu_capable else '❌'}")
        logger.info(f"   RAM: {TOTAL_RAM_GB:.1f}GB {'✅' if ram_capable else '❌'}")
        return False

# MODALITÀ AUTOMATICA - sempre al massimo delle performance disponibili
HIGH_PERFORMANCE_MODE = detect_high_performance_capability()

if HIGH_PERFORMANCE_MODE:
    # Configurazione AGGRESSIVA per server potenti
    NUM_THREADS = CPU_CORES * 2        # Sfrutta hyperthreading
    NUM_WORKERS = min(4, CPU_CORES // 2)  # Multi-processo controllato
    INITIAL_CHUNK_SIZE = 200_000       # Chunk molto più grandi
    MAX_CHUNK_SIZE = 500_000           # Limite massimo aumentato
    INSERT_BATCH_SIZE_MULTIPLIER = 3   # Batch MySQL più grandi
    CONNECTION_POOL_SIZE = NUM_WORKERS + 2  # Pool connessioni più ampio
    logger.info("💪 MODALITÀ HIGH-PERFORMANCE AUTO-ATTIVATA!")
    logger.info(f"   CPU: {CPU_CORES} core → {NUM_THREADS} thread aggressivi")
    logger.info(f"   Workers: {NUM_WORKERS} processi paralleli")
    logger.info(f"   Chunk: {INITIAL_CHUNK_SIZE:,} iniziale, max {MAX_CHUNK_SIZE:,}")
    logger.info(f"   INSERT batch: 3x più grandi (fino a 3M record)")
else:
    # Configurazione standard ottimizzata
    NUM_THREADS = max(4, CPU_CORES - 1)  # Usa tutti i core meno 1, minimo 4
    NUM_WORKERS = 1   # MONO-PROCESSO per sistemi standard
    # Calcolo chunk size ottimizzato per sistema standard
    INITIAL_CHUNK_SIZE = max(5000, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_INIT_RATIO) / AVG_RECORD_SIZE_BYTES))
    MAX_CHUNK_SIZE = max(INITIAL_CHUNK_SIZE, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_MAX_RATIO) / AVG_RECORD_SIZE_BYTES))
    INSERT_BATCH_SIZE_MULTIPLIER = 1
    CONNECTION_POOL_SIZE = 2
    logger.info("🏃 MODALITÀ STANDARD AUTO-ATTIVATA")
    logger.info(f"   CPU: {CPU_CORES} core → {NUM_THREADS} thread")
    logger.info(f"   Configurazione ottimizzata per sistema standard")

# Calcola la RAM totale del sistema - aggressivo ma sicuro
TOTAL_MEMORY_BYTES = psutil.virtual_memory().total
TOTAL_MEMORY_GB = TOTAL_MEMORY_BYTES / (1024 ** 3)
MEMORY_BUFFER_RATIO = 0.2  # Solo 20% libero, 80% usabile
USABLE_MEMORY_BYTES = int(TOTAL_MEMORY_BYTES * (1 - MEMORY_BUFFER_RATIO))
USABLE_MEMORY_GB = USABLE_MEMORY_BYTES / (1024 ** 3)

# Chunk size pi� aggressivo con pi� RAM
CHUNK_SIZE_INIT_RATIO = 0.10   # 10% della RAM usabile (raddoppiato)
CHUNK_SIZE_MAX_RATIO = 0.25    # 25% della RAM usabile (aumentato)
AVG_RECORD_SIZE_BYTES = 2 * 1024  # Stimiamo 2KB per record
INITIAL_CHUNK_SIZE = max(5000, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_INIT_RATIO) / AVG_RECORD_SIZE_BYTES))
MAX_CHUNK_SIZE = max(INITIAL_CHUNK_SIZE, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_MAX_RATIO) / AVG_RECORD_SIZE_BYTES))
MIN_CHUNK_SIZE = 5000  # Aumentato da 1000 a 5000

# Chunk size massimo MOLTO pi� aggressivo
MAX_CHUNK_SIZE = min(MAX_CHUNK_SIZE, 150000)  # Aumentato da 75k a 150k

# Parametri per analisi schema (configurabili per velocità)
SCHEMA_ANALYSIS_MODE = os.environ.get('SCHEMA_ANALYSIS_MODE', 'fast')  # 'fast', 'ultra-fast', 'thorough'

# Configurazione analisi basata sulla modalità
if SCHEMA_ANALYSIS_MODE == 'ultra-fast':
    SCHEMA_MAX_ROWS_PER_FILE = 20
    SCHEMA_MAX_FILES_PER_CATEGORY = 5
    SCHEMA_MAX_SAMPLES_PER_FIELD = 5
elif SCHEMA_ANALYSIS_MODE == 'fast':
    SCHEMA_MAX_ROWS_PER_FILE = 50
    SCHEMA_MAX_FILES_PER_CATEGORY = 10
    SCHEMA_MAX_SAMPLES_PER_FIELD = 10
else:  # thorough
    SCHEMA_MAX_ROWS_PER_FILE = 200
    SCHEMA_MAX_FILES_PER_CATEGORY = 20
    SCHEMA_MAX_SAMPLES_PER_FIELD = 20

def log_memory_status(logger_instance: logging.Logger, context: str = "") -> None:
    """Helper per logging status memoria."""
    memory_info = psutil.virtual_memory()
    used_gb = memory_info.used / (1024**3)
    total_gb = memory_info.total / (1024**3)
    usage_pct = memory_info.percent
    available_gb = memory_info.available / (1024**3)
    
    prefix = f"[{context}] " if context else ""
    logger_instance.info(f"[RAM] {prefix}RAM: {used_gb:.1f}GB/{total_gb:.1f}GB ({usage_pct:.1f}%) | Disponibile: {available_gb:.1f}GB")

def log_performance_stats(logger_instance: logging.Logger, operation: str, count: int, elapsed_time: float, context: str = "") -> None:
    """Helper per logging statistiche performance."""
    speed = count / elapsed_time if elapsed_time > 0 else 0
    prefix = f"[{context}] " if context else ""
    logger_instance.info(f"[PERF] {prefix}{operation}: {count:,} elementi in {elapsed_time:.1f}s ({speed:.1f} el/s)")

def log_file_progress(logger_instance: logging.Logger, current: int, total: int, file_name: str = "", extra_info: str = "") -> None:
    """Helper per logging progresso file."""
    pct = (current / total * 100) if total > 0 else 0
    file_info = f" - {file_name}" if file_name else ""
    extra = f" | {extra_info}" if extra_info else ""
    logger_instance.info(f"[PROG] Progresso: {current}/{total} ({pct:.1f}%){file_info}{extra}")

def log_batch_progress(logger_instance: logging.Logger, processed: int, total: int, speed: Optional[float] = None, memory_info: Optional[str] = None) -> None:
    """Helper per logging progresso batch con informazioni opzionali."""
    pct = (processed / total * 100) if total > 0 else 0
    speed_info = f" | {speed:.0f} rec/s" if speed else ""
    memory_info_str = f" | RAM: {memory_info}" if memory_info else ""
    logger_instance.info(f"[BATCH] Batch: {processed:,}/{total:,} ({pct:.1f}%){speed_info}{memory_info_str}")

def log_error_with_context(logger_instance: logging.Logger, error: Exception, context: str = "", operation: str = "") -> None:
    """Log an error with additional context and operation information."""
    context_str = f"[{context}] " if context else ""
    operation_str = f" durante {operation}" if operation else ""

    actual_error_type_name = type(error).__name__
    # Robust check for InterfaceError
    is_interface_error = (
        actual_error_type_name == 'MySQLInterfaceError' or
        actual_error_type_name == 'InterfaceError' or
        hasattr(error, '__module__') and error.__module__.startswith('mysql.connector') and 'InterfaceError' in actual_error_type_name
    )

    # Debug log to see what type is being processed
    logger_instance.info(f"DEBUG log_error_with_context (import_json_mysql): Processing error of type '{actual_error_type_name}', detected as InterfaceError: {is_interface_error}")

    if is_interface_error:
        errno = getattr(error, 'errno', 'N/A')
        sqlstate = getattr(error, 'sqlstate', 'N/A')
        safe_error_message = (
            f"A MySQL Interface Error occurred (type: {actual_error_type_name}, errno: {errno}, sqlstate: {sqlstate}). "
            f"Operation failed. Original message details suppressed due to formatting issues." 
        )
        logger_instance.error(f"[ERROR] {context_str}Errore{operation_str}: {safe_error_message}")
    else:
        logger_instance.error(f"[ERROR] {context_str}Errore{operation_str}: {error}")

def log_resource_optimization(logger_instance: logging.Logger) -> None:
    """Helper per logging configurazione risorse ottimizzate."""
    logger_instance.info("[CONFIG] Configurazione risorse DINAMICHE ottimizzate:")
    logger_instance.info(f"   [CPU] CPU: {CPU_CORES} core -> {NUM_THREADS} thread attivi ({(NUM_THREADS/CPU_CORES*100):.0f}% utilizzo)")
    logger_instance.info(f"   [RAM] RAM totale: {TOTAL_MEMORY_GB:.1f}GB")
    logger_instance.info(f"   [RAM] RAM usabile: {USABLE_MEMORY_GB:.1f}GB (buffer {MEMORY_BUFFER_RATIO*100:.0f}%)")
    logger_instance.info(f"   [PROC] Worker process: {NUM_WORKERS} (MONO-PROCESSO + thread aggressivi)")
    logger_instance.info(f"   [BATCH] Batch size principale: {BATCH_SIZE:,}")
    
    current_insert_batch = calculate_dynamic_insert_batch_size()
    current_ram = psutil.virtual_memory().available / (1024**3)
    logger_instance.info(f"   [INSERT] INSERT batch dinamico: {current_insert_batch:,} (RAM disponibile: {current_ram:.1f}GB)")
    logger_instance.info(f"   [CHUNK] Chunk size max: {MAX_CHUNK_SIZE:,}")

def handle_data_too_long_error(cursor, error_message, table_name):
    """Gestisce errori di 'Data too long' modificando la colonna da VARCHAR a TEXT."""
    # Estrai il nome della colonna dall'errore
    match = re.search(r"Data too long for column '([^']+)'", error_message)
    if not match:
        return False
        
    column_name = match.group(1)
    logger.warning(f"[ADAPT] Colonna '{column_name}' troppo piccola, converto a TEXT...")
    
    try:
        # Modifica la colonna da VARCHAR a TEXT
        alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} TEXT"
        cursor.execute(alter_query)
        logger.info(f"[OK] Colonna '{column_name}' convertita a TEXT")
        return True
    except Exception as e:
        logger.error(f"[ERROR] Errore nella modifica della colonna '{column_name}': {e}")
        return False

def handle_type_compatibility_error(cursor, error_message, table_name):
    """Gestisce errori di incompatibilità di tipo modificando dinamicamente la struttura."""
    
    # Pattern per diversi tipi di errore MySQL
    patterns = {
        'incorrect_integer': r"Incorrect integer value '([^']+)' for column '([^']+)'",
        'incorrect_decimal': r"Incorrect decimal value '([^']+)' for column '([^']+)'",
        'incorrect_date': r"Incorrect date value '([^']+)' for column '([^']+)'",
        'incorrect_datetime': r"Incorrect datetime value '([^']+)' for column '([^']+)'",
        'out_of_range': r"Out of range value for column '([^']+)'",
        'data_truncated': r"Data truncated for column '([^']+)'",
    }
    
    column_name = None
    error_type = None
    problematic_value = None
    
    # Identifica il tipo di errore e la colonna
    for error_type_name, pattern in patterns.items():
        match = re.search(pattern, error_message)
        if match:
            if error_type_name == 'out_of_range':
                column_name = match.group(1)
                problematic_value = "out_of_range"
            elif error_type_name == 'data_truncated':
                column_name = match.group(1)
                problematic_value = "data_truncated"
            else:
                # For patterns with two groups (value, column)
                problematic_value = match.group(1)
                column_name = match.group(2)
            error_type = error_type_name
            break
    
    if not column_name or not error_type:
        return False
    
    logger.warning(f"[ADAPT] Errore di tipo '{error_type}' per colonna '{column_name}' con valore '{problematic_value}'")
    
    try:
        # Strategia di adattamento intelligente basata sul valore problematico
        if error_type in ['incorrect_integer', 'out_of_range']:
            # Analizza il valore per determinare il tipo più appropriato
            if problematic_value and problematic_value != "out_of_range":
                # Controlla se è un flag (Y/N/S)
                if column_name and 'flag_' in column_name.lower() and problematic_value.upper() in ['Y', 'N', 'S', 'T', 'F']:
                    alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} CHAR(1)"
                    logger.info(f"[ADAPT] Rilevato flag in '{column_name}' -> CHAR(1)")
                # Controlla se è un codice fiscale
                elif re.match(r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$', problematic_value):
                    alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR(16)"
                    logger.info(f"[ADAPT] Rilevato codice fiscale in '{column_name}' -> VARCHAR(16)")
                # Controlla se è un CIG
                elif re.match(r'^[A-Z0-9]{10}$', problematic_value):
                    alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR(10)"
                    logger.info(f"[ADAPT] Rilevato CIG in '{column_name}' -> VARCHAR(10)")
                # Controlla se è una partita IVA
                elif re.match(r'^\d{11}$', problematic_value):
                    alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR(11)"
                    logger.info(f"[ADAPT] Rilevata P.IVA in '{column_name}' -> VARCHAR(11)")
                # Controlla se è un codice alfanumerico
                elif re.match(r'^[A-Z0-9-_]+$', problematic_value, re.IGNORECASE):
                    max_len = max(50, len(problematic_value) + 10)  # Margine di sicurezza
                    alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR({max_len})"
                    logger.info(f"[ADAPT] Rilevato codice alfanumerico in '{column_name}' -> VARCHAR({max_len})")
                else:
                    # Fallback generico per valori numerici non standard
                    alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR(255)"
                    logger.info(f"[ADAPT] Converto colonna numerica '{column_name}' a VARCHAR(255)")
            else:
                alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR(255)"
                logger.info(f"[ADAPT] Converto colonna numerica '{column_name}' a VARCHAR(255)")
            
        elif error_type == 'incorrect_decimal':
            # Da DECIMAL a VARCHAR per gestire formati diversi
            alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR(255)"
            logger.info(f"[ADAPT] Converto colonna decimale '{column_name}' a VARCHAR(255)")
            
        elif error_type in ['incorrect_date', 'incorrect_datetime']:
            # Da DATE/DATETIME a VARCHAR per gestire formati diversi
            alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR(50)"
            logger.info(f"[ADAPT] Converto colonna data '{column_name}' a VARCHAR(50)")
            
        elif error_type == 'data_truncated':
            # Aumenta la dimensione o converti a TEXT
            alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} TEXT"
            logger.info(f"[ADAPT] Converto colonna troncata '{column_name}' a TEXT")
            
        else:
            # Fallback generico: converti a TEXT
            alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} TEXT"
            logger.info(f"[ADAPT] Converto colonna '{column_name}' a TEXT (fallback)")
        
        cursor.execute(alter_query)
        logger.info(f"[SUCCESS] Colonna '{column_name}' adattata con successo")
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Errore nell'adattamento della colonna '{column_name}': {e}")
        return False

def handle_duplicate_key_error(cursor, error_message, table_name):
    """Gestisce errori di chiave duplicata con strategia IGNORE."""
    logger.warning(f"[DUPLICATE] Rilevata chiave duplicata in {table_name}: {error_message}")
    logger.info(f"[STRATEGY] Userò INSERT IGNORE per saltare i duplicati")
    return True  # Indica che l'errore può essere gestito

def adaptive_insert_with_retry(cursor, query, data, table_name, max_retries=3):
    """
    Inserisce dati con retry automatico e adattamento della struttura in caso di errori.
    
    Questa funzione è intelligente e si adatta automaticamente ai problemi di compatibilità
    modificando la struttura della tabella quando necessario.
    """
    
    for attempt in range(max_retries):
        try:
            # Per la prima volta, prova INSERT normale
            current_query = query
            
            # Se è un retry per duplicati, usa direttamente INSERT IGNORE
            if attempt > 0 and "INSERT INTO" in query:
                current_query = query.replace("INSERT INTO", "INSERT IGNORE INTO", 1)
                logger.info(f"[RETRY-IGNORE] Tentativo {attempt + 1}: uso INSERT IGNORE per evitare duplicati")
            
            # Tenta l'inserimento
            if isinstance(data, list) and len(data) > 0:
                cursor.executemany(current_query, data)
            else:
                cursor.execute(current_query, data)
            
            # Se arriviamo qui, l'inserimento è riuscito
            if attempt > 0:
                logger.info(f"[SUCCESS] Inserimento riuscito al tentativo {attempt + 1}")
            return True
            
        except mysql.connector.Error as e:
            logger.warning(f"[RETRY] Tentativo {attempt + 1}/{max_retries} fallito: {e}")
            
            adapted = False
            
            # Errore 1406: Data too long
            if e.errno == 1406:
                adapted = handle_data_too_long_error(cursor, str(e), table_name)
            
            # Errore 1062: Duplicate entry (chiave duplicata)
            elif e.errno == 1062:
                adapted = handle_duplicate_key_error(cursor, str(e), table_name)
                if adapted:
                    # Converti la query da INSERT a INSERT IGNORE per il prossimo tentativo
                    logger.info(f"[ADAPT] Prossimo tentativo userà INSERT IGNORE per gestire duplicati")
                    # Non riproviamo subito, lasciamo che il loop principale riprovi
                    continue
            
            # Errore 1264: Out of range value
            elif e.errno == 1264:
                adapted = handle_type_compatibility_error(cursor, str(e), table_name)
            
            # Errore 1366: Incorrect string/integer/decimal/date value
            elif e.errno == 1366:
                adapted = handle_type_compatibility_error(cursor, str(e), table_name)
            
            # Errore 1265: Data truncated (IL PIÙ COMUNE)
            elif e.errno == 1265:
                adapted = handle_type_compatibility_error(cursor, str(e), table_name)
                
                # STRATEGIA SPECIALE per importo_aggiudicazione che continua a fallire
                if "importo_aggiudicazione" in str(e):
                    logger.warning(f"[SPECIAL] Problema ricorrente con importo_aggiudicazione, forzo conversione a TEXT")
                    try:
                        cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN importo_aggiudicazione TEXT")
                        logger.info(f"[FORCE-FIX] Colonna importo_aggiudicazione convertita a TEXT")
                        adapted = True
                    except Exception as alter_error:
                        logger.error(f"[FORCE-FIX] Errore nella conversione forzata: {alter_error}")
            
            # Altri errori specifici che possono essere adattati
            elif "Incorrect" in str(e) or "Data truncated" in str(e):
                adapted = handle_type_compatibility_error(cursor, str(e), table_name)
            
            if adapted:
                logger.info(f"[ADAPT] Struttura adattata, riprovo inserimento...")
                # Continua il loop per riprovare con la struttura modificata
                continue
            else:
                # Se non riusciamo ad adattare, e siamo all'ultimo tentativo, rilancia l'errore
                if attempt == max_retries - 1:
                    logger.error(f"[FAIL] Impossibile adattare la struttura dopo {max_retries} tentativi")
                    # INVECE DI FALLIRE COMPLETAMENTE, ritorna False per permettere di saltare questo mini-batch
                    return False
                else:
                    # Aspetta un po' prima di riprovare
                    time.sleep(1)
        
        except Exception as e:
            logger.error(f"[UNEXPECTED] Errore inaspettato nel tentativo {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                return False
            time.sleep(1)
    
    return False

class AdaptiveBatchSizer:
    """Gestisce dinamicamente la dimensione del batch basandosi sull'utilizzo della RAM."""
    
    def __init__(self, initial_batch_size, target_ram_usage=None):
        self.initial_batch_size = initial_batch_size
        self.current_batch_size = initial_batch_size
        self.target_ram_usage = target_ram_usage or TARGET_RAM_USAGE  # Usa il target dinamico
        self.adjustments = 0
        
    def adjust_batch_size(self, current_ram_usage, processing_speed):
        """
        Aggiusta il batch size basandosi su utilizzo RAM e velocità di processing.
        
        Args:
            current_ram_usage: Percentuale di RAM utilizzata (0.0-1.0)
            processing_speed: Record processati al secondo
        """
        # Calcola un average rolling delle ultime 3 misurazioni
        if not hasattr(self, 'ram_history'):
            self.ram_history = []
        
        self.ram_history.append(current_ram_usage)
        if len(self.ram_history) > 3:
            self.ram_history.pop(0)
            
        avg_ram_usage = sum(self.ram_history) / len(self.ram_history)
        
        old_batch_size = self.current_batch_size
        
        # Strategia AGGRESSIVA per HIGH-PERFORMANCE
        if HIGH_PERFORMANCE_MODE:
            # In modalità HIGH-PERFORMANCE siamo più aggressivi
            if avg_ram_usage < (self.target_ram_usage * 0.70):  # < 63% per HP mode
                # Sotto il target, aumenta aggressivamente
                self.current_batch_size = min(int(self.current_batch_size * 1.5), 5_000_000)
            elif avg_ram_usage < (self.target_ram_usage * 0.85):  # < 76.5% per HP mode
                # Vicino al target, aumenta moderatamente
                self.current_batch_size = min(int(self.current_batch_size * 1.2), 3_000_000)
            elif avg_ram_usage > (self.target_ram_usage * 0.95):  # > 85.5% per HP mode
                # Troppo vicino al limite, riduci
                self.current_batch_size = max(int(self.current_batch_size * 0.8), 50_000)
        else:
            # Strategia standard ottimizzata
            if avg_ram_usage < (self.target_ram_usage * 0.60):  # < 48% per standard
                # Sotto il target, aumenta aggressivamente
                self.current_batch_size = min(int(self.current_batch_size * 1.3), 2_000_000)
            elif avg_ram_usage < (self.target_ram_usage * 0.75):  # < 60% per standard
                # Vicino al target, aumenta moderatamente
                self.current_batch_size = min(int(self.current_batch_size * 1.1), 1_500_000)
            elif avg_ram_usage > (self.target_ram_usage * 0.90):  # > 72% per standard
                # Troppo vicino al limite, riduci
                self.current_batch_size = max(int(self.current_batch_size * 0.85), 25_000)
        
        if old_batch_size != self.current_batch_size:
            self.adjustments += 1
            mode_icon = "💪" if HIGH_PERFORMANCE_MODE else "🏃"
            logger.info(f"{mode_icon} [ADAPT] Batch size: {old_batch_size:,} → {self.current_batch_size:,} "
                       f"(RAM: {avg_ram_usage:.1%}, target: {self.target_ram_usage:.1%})")
        
        return self.current_batch_size

def calculate_dynamic_insert_batch_size():
    """
    Calcola dinamicamente la dimensione del batch per INSERT MySQL
    basandosi sulla RAM disponibile e sulla modalità operativa.
    """
    # Memoria disponibile in GB
    available_memory_gb = USABLE_MEMORY_GB
    
    # Calcolo base del batch size
    if available_memory_gb >= 12:
        base_batch_size = 1_000_000
    elif available_memory_gb >= 8:
        base_batch_size = 750_000
    elif available_memory_gb >= 4:
        base_batch_size = 500_000
    else:
        base_batch_size = 250_000
    
    # Applica il moltiplicatore per la modalità high-performance
    final_batch_size = int(base_batch_size * INSERT_BATCH_SIZE_MULTIPLIER)
    
    logger.info(f"[DYNAMIC] INSERT batch size: {final_batch_size:,} (base: {base_batch_size:,}, multiplier: {INSERT_BATCH_SIZE_MULTIPLIER}x)")
    
    return final_batch_size

def proactive_schema_fixes(cursor, table_name):
    """Applica fix proattivi per colonne che spesso causano problemi."""
    try:
        # Fix comuni per colonne problematiche dell'ANAC
        common_fixes = [
            ("importo_aggiudicazione", "TEXT", "Importi possono essere molto vari e lunghi"),
            ("importo_lotto", "TEXT", "Importi lotto spesso problematici"),
            ("importo_totale", "TEXT", "Importi totali vari"),
            ("descrizione", "LONGTEXT", "Descrizioni possono essere molto lunghe"),
            ("oggetto", "LONGTEXT", "Oggetti possono essere molto lunghi"),
            ("ragione_sociale", "TEXT", "Ragioni sociali varie"),
            ("denominazione", "TEXT", "Denominazioni varie"),
            ("codice_fiscale", "VARCHAR(16)", "Codici fiscali standard"),
            ("partita_iva", "VARCHAR(11)", "Partite IVA standard"),
            ("cig", "VARCHAR(20)", "CIG possono variare"),
            ("cup", "VARCHAR(20)", "CUP possono variare"),
            ("flag_quote", "CHAR(1)", "Flag quote sono Y/N/S"),
            ("flag_ricorso", "CHAR(1)", "Flag ricorso sono Y/N"),
            ("flag_subappalto", "CHAR(1)", "Flag subappalto sono Y/N"),
            ("flag_servizi", "CHAR(1)", "Flag servizi sono Y/N"),
            ("flag_forniture", "CHAR(1)", "Flag forniture sono Y/N"),
            ("flag_lavori", "CHAR(1)", "Flag lavori sono Y/N"),
            ("numero_gara", "TEXT", "Numeri gara possono essere molto lunghi"),
            ("numero_lotto", "TEXT", "Numeri lotto possono essere molto lunghi"),
            ("descrizione_gara", "LONGTEXT", "Descrizioni gara molto lunghe"),
        ]
        
        # Ottieni la lista delle colonne esistenti
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        existing_columns = {row[0] for row in cursor.fetchall()}
        
        fixes_applied = 0
        for column_name, new_type, reason in common_fixes:
            if column_name in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} {new_type}")
                    logger.info(f"[PROACTIVE] {table_name}.{column_name} → {new_type} ({reason})")
                    fixes_applied += 1
                except Exception as e:
                    # Non loggare errori per fix proattivi, potrebbero essere normali
                    pass
        
        if fixes_applied > 0:
            logger.info(f"[PROACTIVE] Applicati {fixes_applied} fix proattivi per {table_name}")
        
        return fixes_applied
        
    except Exception as e:
        logger.debug(f"[PROACTIVE] Errore in fix proattivi per {table_name}: {e}")
        return 0

def insert_batch_direct(db_connection, main_data, table_name, fields):
    """Inserisce i dati direttamente in MySQL con scrittura progressiva robusta."""
    if not main_data:
        return 0
    
    # Ora db_connection è direttamente la connessione MySQL
    cursor = db_connection.cursor()
    
    # APPLICA FIX PROATTIVI PRIMA DI INIZIARE
    proactive_fixes = proactive_schema_fixes(cursor, table_name)
    if proactive_fixes > 0:
        logger.info(f"[SCHEMA] Applicati {proactive_fixes} fix proattivi per prevenire errori")
        
    # SCRITTURA PROGRESSIVA: mini-batch da 1000 record per commit frequenti
    MINI_BATCH_SIZE = 1000  # Dimensione mini-batch per scrittura sicura
    COMMIT_FREQUENCY = 5    # Commit ogni 5 mini-batch (= ogni 5k record)
    
    # Statistiche RAM dettagliate
    memory_info = psutil.virtual_memory()
    total_ram_gb = memory_info.total / (1024**3)
    used_ram_gb = memory_info.used / (1024**3)
    available_ram_gb = memory_info.available / (1024**3)
    usage_pct = memory_info.percent
    
    logger.info(f"[INSERT] SCRITTURA PROGRESSIVA di {len(main_data):,} record in {table_name}")
    logger.info(f"[INSERT] Mini-batch: {MINI_BATCH_SIZE:,} record | Commit ogni: {COMMIT_FREQUENCY * MINI_BATCH_SIZE:,} record")
    logger.info(f"[RAM] RAM: {used_ram_gb:.1f}GB/{total_ram_gb:.1f}GB utilizzata ({usage_pct:.1f}%) | Disponibile: {available_ram_gb:.1f}GB")
    
    rows_inserted = 0
    mini_batches_processed = 0
    mini_batches_failed = 0
    
    # Prepara la query INSERT
    placeholders = ', '.join(['%s'] * len(fields))
    insert_query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
    
    insert_start_time = time.time()
    last_commit_time = time.time()
    
    try:
        # Processa i dati in mini-batch con commit frequenti
        i = 0
        while i < len(main_data):
            mini_batch_start_time = time.time()
            mini_batch = main_data[i:i + MINI_BATCH_SIZE]
            mini_batch_size = len(mini_batch)
            mini_batches_processed += 1
            
            try:
                # Usa il sistema di inserimento adattivo per questo mini-batch
                success = adaptive_insert_with_retry(cursor, insert_query, mini_batch, table_name, max_retries=2)
                
                if success:
                    rows_inserted += mini_batch_size
                    
                    # COMMIT FREQUENTE: ogni N mini-batch
                    if mini_batches_processed % COMMIT_FREQUENCY == 0:
                        db_connection.commit()
                        commit_time = time.time() - last_commit_time
                        last_commit_time = time.time()
                        
                        logger.info(f"[COMMIT] Salvati {rows_inserted:,} record ({mini_batches_processed} mini-batch) in {commit_time:.1f}s")
                    
                    # Log progresso ogni 10 mini-batch
                    if mini_batches_processed % 10 == 0:
                        elapsed_total = time.time() - insert_start_time
                        avg_speed = rows_inserted / elapsed_total if elapsed_total > 0 else 0
                        progress_pct = (i + mini_batch_size) / len(main_data) * 100
                        
                        logger.info(f"[PROGRESS] {progress_pct:.1f}% - {rows_inserted:,}/{len(main_data):,} record | "
                                  f"Velocità: {avg_speed:.0f} rec/s | Falliti: {mini_batches_failed} mini-batch")
                else:
                    mini_batches_failed += 1
                    logger.warning(f"[SKIP] Mini-batch {mini_batches_processed} fallito ({mini_batch_size:,} record), continuo con il prossimo")
                    
            except Exception as e:
                mini_batches_failed += 1
                logger.error(f"[ERROR] Errore in mini-batch {mini_batches_processed}: {e}")
                # Continua con il prossimo mini-batch invece di fallire tutto
                continue
            
            i += MINI_BATCH_SIZE
        
        # COMMIT FINALE per gli ultimi record
        db_connection.commit()
        
        total_time = time.time() - insert_start_time
        avg_speed = rows_inserted / total_time if total_time > 0 else 0
        final_ram_gb = psutil.virtual_memory().used / (1024**3)
        final_usage_pct = psutil.virtual_memory().percent
        success_rate = (mini_batches_processed - mini_batches_failed) / mini_batches_processed * 100 if mini_batches_processed > 0 else 0
        
        logger.info(f"[COMPLETE] SCRITTURA PROGRESSIVA completata!")
        logger.info(f"[STATS] Record inseriti: {rows_inserted:,}/{len(main_data):,} ({(rows_inserted/len(main_data)*100):.1f}%)")
        logger.info(f"[STATS] Mini-batch: {mini_batches_processed} totali, {mini_batches_failed} falliti (successo: {success_rate:.1f}%)")
        logger.info(f"[PERF] Velocità media: {avg_speed:.0f} rec/s in {total_time:.1f}s")
        logger.info(f"[RAM] RAM finale: {final_ram_gb:.1f}GB ({final_usage_pct:.1f}%)")
        
        return rows_inserted
        
    except Exception as e:
        # Commit finale anche in caso di errore per salvare quello che si può
        try:
            db_connection.commit()
            logger.info(f"[RECOVERY] Commit di emergenza eseguito: {rows_inserted:,} record salvati")
        except:
            pass
        
        logger.error(f"[ERROR] Errore in SCRITTURA PROGRESSIVA: {e}")
        raise
    finally:
        cursor.close()

def process_batch(db_connection, batch, table_definitions, batch_id, progress_tracker=None, category=None):
    if not batch:
        return True  # Empty batch is considered successful

    file_name = batch[0][1]
    # Ora db_connection è direttamente la connessione MySQL
    cursor = db_connection.cursor()
    
    # Determina il nome della tabella per questa categoria
    table_name = f"{category}_data" if category else "main_data"
    
    with LogContext(batch_logger, f"processing batch", 
                   batch_size=len(batch), file=file_name, batch_id=batch_id, category=category, table=table_name):
        try:
            # Ottieni il mapping dei campi per questa categoria specifica
            try:
                if category:
                    # Usa la tabella category_field_mapping per le categorie
                    cursor.execute("""
                        SELECT original_name, sanitized_name 
                        FROM category_field_mapping 
                        WHERE category = %s
                    """, (category,))
                    field_mapping = dict(cursor.fetchall())
                    
                    # Se non troviamo mapping per questa categoria, crea un mapping base
                    if not field_mapping:
                        batch_logger.warning(f"Nessun mapping trovato per categoria '{category}', uso mapping diretto")
                        field_mapping = {field: field.lower().replace(' ', '_') for field in table_definitions.keys()}
                else:
                    # Fallback per tabella field_mapping generica (per compatibilità)
                    cursor.execute("SELECT original_name, sanitized_name FROM field_mapping")
                    field_mapping = dict(cursor.fetchall())
                    
            except mysql.connector.Error as e:
                if e.errno == 2006:  # MySQL server has gone away
                    batch_logger.warning("Connessione persa, riprovo...")
                    raise ValueError("CONNECTION_LOST")
                elif e.errno == 1146:  # Table doesn't exist
                    batch_logger.warning(f"Tabella mapping non trovata, creo mapping diretto per categoria '{category}'")
                    # Crea un mapping diretto come fallback
                    field_mapping = {field: field.lower().replace(' ', '_') for field in table_definitions.keys()}
                else:
                    raise
            
            start_time = time.time()
            
            # Elabora il batch in parallelo
            main_data, json_data = process_batch_parallel(batch, table_definitions, field_mapping, file_name, batch_id)
            
            if main_data:
                # Prepara i campi per INSERT diretto
                fields = ['cig'] + [field_mapping[field] for field in table_definitions.keys() if field.lower() != 'cig'] + ['source_file', 'batch_id']
                
                # Debug: mostra alcuni esempi di dati
                batch_logger.info(f"Dati per INSERT diretto in tabella '{table_name}':")
                batch_logger.info(f"   Campi totali: {len(fields)}")
                batch_logger.info(f"   Record totali: {len(main_data):,}")
                
                # Mostra i primi 2 record per debug
                for i, record in enumerate(main_data[:2]):
                    batch_logger.info(f"   Record {i+1}: {len(record)} valori")
                    for j, (field, value) in enumerate(zip(fields, record)):
                        if 'data_' in field.lower() and j < 10:  # Solo primi 10 campi data
                            batch_logger.info(f"     - {field}: {value} ({type(value).__name__})")
                
                # Inserimento diretto in MySQL nella tabella corretta per categoria
                rows_affected = insert_batch_direct(db_connection, main_data, table_name, fields)
                
                # CRITICAL CHECK: Verify all records were inserted
                expected_records = len(main_data)
                if rows_affected != expected_records:
                    batch_logger.error(f"[BATCH-FAIL] Record inseriti mismatch: {rows_affected:,}/{expected_records:,} ({((rows_affected/expected_records)*100):.1f}%)")
                    return False
                else:
                    batch_logger.info(f"[BATCH-SUCCESS] Tutti i record inseriti correttamente: {rows_affected:,}/{expected_records:,}")
            
            # Gestisci i dati JSON separatamente (manteniamo l'approccio precedente per i JSON)
            if json_data:
                json_errors = []  # Track errors from JSON insertion threads
                
                def insert_json_data(field, data):
                    try:
                        json_data_with_metadata = [(cig, json_str, file_name, batch_id) for cig, json_str in data]
                        insert_json = f"""
                        INSERT INTO {field}_data (cig, {field}_json, source_file, batch_id)
                        VALUES (%s, %s, %s, %s) AS new_data
                        ON DUPLICATE KEY UPDATE
                            {field}_json = new_data.{field}_json,
                            source_file = new_data.source_file,
                            batch_id = new_data.batch_id
                        """
                        cursor.executemany(insert_json, json_data_with_metadata)
                    except mysql.connector.Error as e:
                        if e.errno == 2006:  # MySQL server has gone away
                            batch_logger.warning("Connessione persa durante l'inserimento JSON, riprovo...")
                            raise ValueError("CONNECTION_LOST")
                        raise
                    except Exception as e:
                        json_errors.append(f"JSON insertion failed for field {field}: {e}")
                        raise
            
                # Crea e avvia i thread per l'inserimento JSON
                json_threads = []
                for field, data in json_data.items():
                    if data:
                        thread = threading.Thread(target=insert_json_data, args=(field, data))
                        thread.start()
                        json_threads.append(thread)
                
                # Attendi il completamento di tutti i thread JSON
                for thread in json_threads:
                    thread.join()
                
                # Check for JSON insertion errors
                if json_errors:
                    batch_logger.error(f"[JSON-FAIL] Errori durante inserimento JSON: {json_errors}")
                    return False
            
            elapsed_time = time.time() - start_time
            log_performance_stats(batch_logger, f"Batch completato per categoria '{category}'", len(batch), elapsed_time)
            
            # Return True to indicate successful completion
            return True
        
        except mysql.connector.Error as e:
            if e.errno == 1153:  # Packet too large
                batch_logger.warning("Batch troppo grande, riduco la dimensione...")
                raise ValueError("BATCH_TOO_LARGE")
            raise
        except Exception as e:
            log_error_with_context(batch_logger, e, f"batch {batch_id}", "processing")
            raise
        finally:
            cursor.close()

def connect_mysql():
    """
    DEPRECATO: Usa DatabaseManager.get_connection() al posto di questa funzione.
    Wrapper per compatibilit� con codice esistente.
    """
    db_logger.warning("connect_mysql() deprecato - usa DatabaseManager.get_connection()")
    with DatabaseManager.get_connection() as conn:
        return conn

def check_disk_space():
    """Verifica lo spazio disponibile su disco usando pathlib."""
    try:
        # Verifica lo spazio su /database
        database_path = Path('/database')
        if database_path.exists():
            database_stats = os.statvfs(str(database_path))
            database_free_gb = (database_stats.f_bavail * database_stats.f_frsize) / (1024**3)
        else:
            database_free_gb = 0
        
        # Verifica lo spazio su /tmp
        tmp_path = Path('/tmp')
        if tmp_path.exists():
            tmp_stats = os.statvfs(str(tmp_path))
            tmp_free_gb = (tmp_stats.f_bavail * tmp_stats.f_frsize) / (1024**3)
        else:
            tmp_free_gb = 0
        
        # Verifica lo spazio su /database/tmp
        tmp_dir_path = Path('/database/tmp')
        if tmp_dir_path.exists():
            tmp_dir_stats = os.statvfs(str(tmp_dir_path))
            tmp_dir_free_gb = (tmp_dir_stats.f_bavail * tmp_dir_stats.f_frsize) / (1024**3)
        else:
            tmp_dir_free_gb = 0
        
        logger.info("Spazio disco disponibile:")
        logger.info(f"   ?? {database_path}: {database_free_gb:.1f}GB")
        logger.info(f"   ?? {tmp_path}: {tmp_free_gb:.1f}GB")
        logger.info(f"   ?? {tmp_dir_path}: {tmp_dir_free_gb:.1f}GB")
        
        # Avvisa se lo spazio � basso
        if database_free_gb < 10:
            logger.warning(f"?? Spazio disponibile su {database_path} � basso!")
        if tmp_free_gb < 1:
            logger.warning(f"?? Spazio disponibile su {tmp_path} � basso!")
        if tmp_dir_free_gb < 1:
            logger.warning(f"?? Spazio disponibile su {tmp_dir_path} � basso!")
            
        return database_free_gb, tmp_free_gb, tmp_dir_free_gb
    except Exception as e:
        logger.error(f"? Errore nel controllo dello spazio disco: {e}")
        return None, None, None

def debug_field_analysis(field_name, sample_values, final_type):
    """Debug helper per analizzare perché un campo ha ricevuto un certo tipo."""
    logger.debug(f"[DEBUG] Campo '{field_name}' -> {final_type}")
    logger.debug(f"[DEBUG] Campioni di valori:")
    
    # Analizza i primi 5 valori per debug
    for i, value in enumerate(sample_values[:5]):
        if value is not None:
            value_str = str(value)
            
            # Testa pattern specifici
            patterns_matched = []
            
            # Codice fiscale
            if re.match(r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$', value_str):
                patterns_matched.append("fiscal_code")
            
            # CIG
            if re.match(r'^[A-Z0-9]{10}$', value_str):
                patterns_matched.append("cig_code")
                
            # Partita IVA
            if re.match(r'^\d{11}$', value_str):
                patterns_matched.append("partita_iva")
                
            # Numerico
            if re.match(r'^\d+$', value_str):
                patterns_matched.append("pure_integer")
                
            logger.debug(f"[DEBUG]   {i+1}. '{value_str}' -> pattern: {patterns_matched}")
    
    # Avviso se sembra un problema
    if final_type in ['INT', 'SMALLINT', 'BIGINT'] and any(
        isinstance(val, str) and not str(val).isdigit() 
        for val in sample_values[:10] if val is not None
    ):
        logger.warning(f"[WARN] Campo '{field_name}' classificato come {final_type} ma contiene valori non numerici!")

def analyze_json_structure(json_files):
    field_types = defaultdict(lambda: defaultdict(int))
    field_lengths = defaultdict(int)
    field_patterns = defaultdict(set)  # Per memorizzare i pattern dei valori
    field_has_mixed = defaultdict(bool)  # Traccia se un campo ha valori misti
    field_sample_values = defaultdict(list)  # Campioni di valori per analisi più approfondita
    
    with LogContext(analysis_logger, "analisi struttura JSON", files=len(json_files)):
        analysis_logger.info("Analisi INTELLIGENTE per determinare:")
        analysis_logger.info("1. Tipi di dati ottimali per ogni campo")
        analysis_logger.info("2. Lunghezze massime e pattern dei valori")
        analysis_logger.info("3. Compatibilità MySQL e gestione errori")
        analysis_logger.info("4. Adattamento dinamico per robustezza")
        analysis_logger.info("5. Limite: 3k righe per file + analisi campionamento")
        
        total_files = len(json_files)
        files_analyzed = 0
        records_analyzed = 0
        start_time = time.time()
        last_progress_time = time.time()
        progress_interval = 1.0
        
        # Limite righe per file per analisi veloce ma approfondita
        MAX_ROWS_PER_FILE = 3000  # Aumentato da 2k a 3k per maggiore precisione
        
        # Monitoraggio memoria dinamico come failsafe
        process = psutil.Process()
        max_memory_bytes = USABLE_MEMORY_BYTES * 0.9  # 90% del buffer disponibile per l'analisi
        
        analysis_logger.info(f"Limite righe per file: {MAX_ROWS_PER_FILE:,}")
        log_memory_status(analysis_logger, "limite massimo analisi")
        
        # Pattern migliorati e più intelligenti per identificare i tipi
        patterns = {
            # Tipi numerici precisi
            'pure_integer': r'^\d+$',                    # Solo numeri interi puri
            'negative_integer': r'^-\d+$',               # Numeri negativi
            'pure_decimal': r'^\d+[.,]\d+$',            # Solo decimali puri
            'negative_decimal': r'^-\d+[.,]\d+$',       # Decimali negativi
            'scientific': r'^\d+[.,]?\d*[eE][+-]?\d+$', # Notazione scientifica
            
            # Valori monetari e percentuali
            'monetary_euro': r'^€?\s*\d+([.,]\d{2})?$',  # Euro
            'monetary_dollar': r'^\$?\s*\d+([.,]\d{2})?$', # Dollari
            'percentage': r'^\d+([.,]\d+)?%$',           # Percentuali
            
            # Date e tempi - pattern più precisi
            'date_iso': r'^\d{4}-\d{2}-\d{2}$',         # Date ISO (YYYY-MM-DD)
            'date_european': r'^\d{2}/\d{2}/\d{4}$',    # Date europee (DD/MM/YYYY)
            'date_american': r'^\d{2}/\d{2}/\d{4}$',    # Date americane (MM/DD/YYYY)
            'date_italian': r'^\d{2}-\d{2}-\d{4}$',     # Date italiane (DD-MM-YYYY)
            'datetime_iso': r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', # DateTime ISO
            'datetime_european': r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}', # DateTime europeo
            'time_hhmm': r'^\d{2}:\d{2}$',              # Ore e minuti
            'time_hhmmss': r'^\d{2}:\d{2}:\d{2}$',      # Ore, minuti, secondi
            'timestamp_unix': r'^\d{10,13}$',           # Timestamp Unix
            
            # Booleani e valori speciali
            'boolean_it': r'^(si|no|vero|falso)$',      # Booleani italiani
            'boolean_en': r'^(true|false|yes|no|1|0)$', # Booleani inglesi
            
            # Codici specifici italiani/ANAC
            'fiscal_code': r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$', # Codice fiscale
            'partita_iva': r'^\d{11}$',                 # Partita IVA
            'cup_code': r'^[A-Z]\d{13}$',               # CUP
            'cig_code': r'^[A-Z0-9]{10}$',              # CIG
            'postal_code_it': r'^\d{5}$',               # CAP italiano
            
            # Email, URL, telefoni
            'email': r'^[^@]+@[^@]+\.[^@]+$',           # Email
            'url': r'^https?://',                        # URL
            'phone_it': r'^(\+39\s?)?(\d{2,4}[\s-]?)?\d{6,10}$', # Telefoni italiani
            
            # Alfanumerici e codici misti
            'alphanumeric_upper': r'^[A-Z0-9]+$',       # Solo maiuscole e numeri
            'alphanumeric_mixed': r'^[A-Za-z0-9]+$',    # Lettere e numeri misti
            'code_with_dashes': r'^[A-Z0-9-]+$',        # Codici con trattini
            
            # Testi e descrizioni
            'text_short': r'^.{1,50}$',                 # Testo corto
            'text_medium': r'^.{51,255}$',              # Testo medio
            'text_long': r'^.{256,}$',                  # Testo lungo
        }
        
        def analyze_value_type(value):
            """Analizza un singolo valore e determina il suo tipo più specifico."""
            if value is None or value == '':
                return 'null'
            
            if isinstance(value, bool):
                return 'boolean_native'
            if isinstance(value, int):
                return 'integer_native'
            if isinstance(value, float):
                return 'decimal_native'
            if isinstance(value, (list, dict)):
                return 'json_native'
                
            value_str = str(value).strip()
            if not value_str:
                return 'empty'
            
            # PRIORITÀ ALTA: Pattern specifici prima dei generici
            # Codici fiscali, CIG, P.IVA hanno precedenza sui pattern numerici
            high_priority_patterns = [
                'fiscal_code', 'cig_code', 'cup_code', 'partita_iva', 
                'email', 'url', 'phone_it', 'postal_code_it'
            ]
            
            # Testa prima i pattern ad alta priorità
            for pattern_name in high_priority_patterns:
                if pattern_name in patterns:
                    if re.match(patterns[pattern_name], value_str, re.IGNORECASE):
                        return pattern_name
            
            # Poi testa tutti gli altri pattern in ordine di specificità
            for pattern_name, pattern in patterns.items():
                if pattern_name not in high_priority_patterns:
                    if re.match(pattern, value_str, re.IGNORECASE):
                        return pattern_name
            
            # Se nessun pattern specifico, classifica come testo
            length = len(value_str)
            if length <= 50:
                return 'text_short'
            elif length <= 255:
                return 'text_medium'
            else:
                return 'text_long'
        
        def determine_mysql_type(field, type_counts, max_length, sample_values):
            """Determina il tipo MySQL più appropriato basato sull'analisi."""
            total_values = sum(type_counts.values())
            if total_values == 0:
                return 'VARCHAR(50)'  # Default fallback
            
            # Ordina i tipi per frequenza
            sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
            primary_type = sorted_types[0][0]
            primary_count = sorted_types[0][1]
            primary_percentage = (primary_count / total_values) * 100
            
            analysis_logger.debug(f"Campo '{field}': tipo primario '{primary_type}' ({primary_percentage:.1f}%)")
            
            # Debug dei tipi rilevati
            analysis_logger.debug(f"Campo '{field}' - distribuzione tipi: {dict(sorted_types[:3])}")
            
            # Logica per determinare il tipo MySQL
            
            # 1. Se > 90% dei valori sono dello stesso tipo, usa quello
            if primary_percentage >= 90:
                
                # Tipi numerici
                if primary_type in ['integer_native', 'pure_integer', 'negative_integer']:
                    # Verifica range per scegliere INT, BIGINT, etc.
                    sample_numbers = [int(v) for v in sample_values if str(v).lstrip('-').isdigit()]
                    if sample_numbers:
                        max_val = max(sample_numbers)
                        min_val = min(sample_numbers)
                        if min_val >= -32768 and max_val <= 32767:
                            mysql_type = 'SMALLINT'
                        elif min_val >= -2147483648 and max_val <= 2147483647:
                            mysql_type = 'INT'
                        else:
                            mysql_type = 'BIGINT'
                    else:
                        mysql_type = 'INT'
                    
                    # Debug per campi numerici
                    debug_field_analysis(field, sample_values, mysql_type)
                    return mysql_type
                
                elif primary_type in ['decimal_native', 'pure_decimal', 'negative_decimal', 'scientific']:
                    return 'DECIMAL(20,6)'  # Precisione alta per decimali
                
                elif primary_type in ['monetary_euro', 'monetary_dollar']:
                    return 'DECIMAL(15,2)'  # Standard per valori monetari
                
                elif primary_type == 'percentage':
                    return 'DECIMAL(5,2)'   # Percentuali 0-100.xx
                
                # Tipi data/tempo
                elif primary_type in ['date_iso', 'date_european', 'date_american', 'date_italian']:
                    return 'DATE'
                
                elif primary_type in ['datetime_iso', 'datetime_european', 'timestamp_unix']:
                    return 'DATETIME'
                
                elif primary_type in ['time_hhmm', 'time_hhmmss']:
                    return 'TIME'
                
                # Booleani
                elif primary_type in ['boolean_native', 'boolean_it', 'boolean_en']:
                    return 'BOOLEAN'
                
                # Codici specifici con lunghezza fissa
                elif primary_type == 'fiscal_code':
                    analysis_logger.info(f"[DETECT] Campo '{field}' riconosciuto come codice fiscale -> VARCHAR(16)")
                    return 'VARCHAR(16)'
                elif primary_type == 'partita_iva':
                    analysis_logger.info(f"[DETECT] Campo '{field}' riconosciuto come P.IVA -> VARCHAR(11)")
                    return 'VARCHAR(11)'
                elif primary_type == 'cup_code':
                    analysis_logger.info(f"[DETECT] Campo '{field}' riconosciuto come CUP -> VARCHAR(15)")
                    return 'VARCHAR(15)'
                elif primary_type == 'cig_code':
                    analysis_logger.info(f"[DETECT] Campo '{field}' riconosciuto come CIG -> VARCHAR(10)")
                    return 'VARCHAR(10)'
                elif primary_type == 'postal_code_it':
                    return 'VARCHAR(5)'
                
                # Email, URL
                elif primary_type == 'email':
                    return 'VARCHAR(255)'
                elif primary_type == 'url':
                    return 'TEXT'  # URL possono essere lunghi
                elif primary_type in ['phone_it']:
                    return 'VARCHAR(20)'
                
                # JSON nativo
                elif primary_type == 'json_native':
                    return 'JSON'
            
            # 2. Se ci sono tipi misti ma prevalentemente numerici
            numeric_types = ['integer_native', 'decimal_native', 'pure_integer', 'pure_decimal', 'negative_integer', 'negative_decimal']
            numeric_percentage = sum(type_counts.get(t, 0) for t in numeric_types) / total_values * 100
            
            if numeric_percentage >= 70:
                # Se la maggior parte sono numeri, usa DECIMAL per sicurezza
                return 'DECIMAL(20,6)'
            
            # 3. Se ci sono molte date miste
            date_types = ['date_iso', 'date_european', 'date_american', 'date_italian', 'datetime_iso', 'datetime_european']
            date_percentage = sum(type_counts.get(t, 0) for t in date_types) / total_values * 100
            
            if date_percentage >= 70:
                return 'DATETIME'  # Tipo più flessibile per date
            
            # 4. Fallback: VARCHAR/TEXT basato sulla lunghezza
            if max_length <= 50:
                return 'VARCHAR(100)'  # Margine di sicurezza
            elif max_length <= 255:
                return 'VARCHAR(500)'  # Margine di sicurezza
            elif max_length <= 1000:
                return 'TEXT'
            else:
                return 'LONGTEXT'
        
        # Analisi dei file
        for json_file in json_files:
            files_analyzed += 1
            file_records = 0
            file_start_time = time.time()
            memory_exceeded = False
            limit_reached = False
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            # Limite principale: 3k righe per file
                            if file_records >= MAX_ROWS_PER_FILE:
                                limit_reached = True
                                break
                                
                            record = json.loads(line.strip())
                            file_records += 1
                            records_analyzed += 1
                            
                            for field, value in record.items():
                                field = field.lower().replace(' ', '_')
                                
                                # Analizza il tipo del valore
                                value_type = analyze_value_type(value)
                                field_types[field][value_type] += 1
                                
                                # Campiona alcuni valori per analisi più approfondita
                                if len(field_sample_values[field]) < 50:  # Max 50 campioni per campo
                                    field_sample_values[field].append(value)
                                
                                # Traccia lunghezza massima per stringhe
                                if isinstance(value, str):
                                    current_length = len(value)
                                    if current_length > field_lengths[field]:
                                        field_lengths[field] = current_length
                            
                            # Controllo memoria ogni 1000 record (failsafe)
                            if file_records % 1000 == 0:
                                if not check_memory_limit():
                                    log_memory_status(memory_logger, "limite raggiunto")
                                    memory_exceeded = True
                                    break
                                
                                # Garbage collection ogni 1000 record
                                gc.collect()
                            
                            current_time = time.time()
                            if current_time - last_progress_time >= progress_interval:
                                elapsed = current_time - file_start_time
                                speed = file_records / elapsed if elapsed > 0 else 0
                                total_elapsed = current_time - start_time
                                avg_speed = records_analyzed / total_elapsed if total_elapsed > 0 else 0
                                    
                                log_file_progress(progress_logger, files_analyzed, total_files, 
                                                    Path(json_file).name, 
                                                    f"Record: {file_records:,}/{MAX_ROWS_PER_FILE:,} | {speed:.1f} rec/s")
                                log_performance_stats(progress_logger, "Analisi totale", records_analyzed, total_elapsed)
                                log_memory_status(memory_logger, "analisi")
                                
                                last_progress_time = current_time
                                
                        except Exception as e:
                            log_error_with_context(analysis_logger, e, "analisi record", f"file {json_file}")
                            continue
                
                file_time = time.time() - file_start_time
                
                # Determina lo status del file
                status = "[MEMORY]" if memory_exceeded else "[LIMIT]" if limit_reached else "[COMPLETE]"
                        
                log_performance_stats(analysis_logger, f"File {status}", file_records, file_time, 
                                        f"{files_analyzed}/{total_files}")
                
                # Garbage collection dopo ogni file
                gc.collect()
                
            except Exception as e:
                log_error_with_context(analysis_logger, e, f"file {json_file}", "analisi")
                continue
        
        # Crea le definizioni delle tabelle con tipi intelligenti
        table_definitions = {}
        total_estimated_row_size = 0
        field_analysis_report = []
        
        for field, type_counts in field_types.items():
            mysql_type = determine_mysql_type(field, type_counts, field_lengths[field], field_sample_values[field])
            table_definitions[field] = mysql_type
            
            # Calcola la dimensione stimata del campo per MySQL
            field_size = estimate_mysql_field_size(mysql_type)
            total_estimated_row_size += field_size
            
            # Prepara report di analisi
            total_values = sum(type_counts.values())
            primary_type = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else 'unknown'
            field_analysis_report.append((field, mysql_type, total_values, primary_type, field_size))
        
        # Garbage collection finale
        gc.collect()
        
        # Report di analisi intelligente
        mysql_row_limit = 65535
        
        analysis_logger.info("="*80)
        analysis_logger.info("[ANALYSIS] Riepilogo analisi intelligente:")
        log_file_progress(analysis_logger, files_analyzed, total_files, "completati")
        log_performance_stats(analysis_logger, "Record analizzati", records_analyzed, time.time() - start_time)
        
        # Gestisci il caso di nessun file analizzato (evita divisione per zero)
        if files_analyzed > 0:
            analysis_logger.info(f"[STATS] Media record per file: {records_analyzed/files_analyzed:.0f}")
        else:
            analysis_logger.warning(f"[STATS] Nessun file analizzato - impossibile calcolare statistiche")
        
        analysis_logger.info(f"[STATS] Campi rilevati: {len(table_definitions)}")
        analysis_logger.info(f"[STATS] Dimensione riga stimata: {total_estimated_row_size:,} bytes")
        log_memory_status(memory_logger, "finale analisi")
        
        # Se non ci sono file, crea una definizione tabella minimale per evitare errori
        if not table_definitions:
            analysis_logger.warning(f"[FALLBACK] Nessun campo rilevato, creo struttura tabella minimale")
            table_definitions = {
                'cig': 'VARCHAR(20)',
                'data_creazione': 'DATETIME', 
                'oggetto': 'TEXT',
                'importo': 'DECIMAL(15,2)'
            }
            analysis_logger.info(f"[FALLBACK] Creata struttura base con {len(table_definitions)} campi essenziali")
        
        # Verifica limite MySQL solo se abbiamo dati
        if total_estimated_row_size > mysql_row_limit:
            analysis_logger.warning(f"[WARN] RIGA TROPPO GRANDE! Supera il limite MySQL di {(total_estimated_row_size - mysql_row_limit):,} bytes")
            # Auto-ottimizzazione: converti campi grandi a TEXT
            optimized_count = 0
            for field, mysql_type in table_definitions.items():
                if mysql_type.startswith('VARCHAR') and '500' in mysql_type:
                    table_definitions[field] = 'TEXT'
                    analysis_logger.info(f"[AUTO-FIX] {field}: {mysql_type} -> TEXT")
                    optimized_count += 1
            
            if optimized_count > 0:
                analysis_logger.info(f"[AUTO-FIX] Ottimizzati {optimized_count} campi per rispettare i limiti MySQL")
        
        # Report dettagliato per categoria solo se abbiamo dati
        if field_analysis_report:
            analysis_logger.info("[REPORT] Analisi per campo:")
            field_analysis_report.sort(key=lambda x: x[4], reverse=True)  # Ordina per dimensione
            for field, mysql_type, total_values, primary_type, size in field_analysis_report[:20]:  # Top 20
                analysis_logger.info(f"  {field}: {mysql_type} ({total_values:,} valori, tipo: {primary_type}, {size}B)")
            
            if len(field_analysis_report) > 20:
                analysis_logger.info(f"  ... e altri {len(field_analysis_report) - 20} campi")
        else:
            analysis_logger.info("[REPORT] Nessun campo da analizzare - utilizzata struttura fallback")
        
        analysis_logger.info("="*80)
        
        return table_definitions

def estimate_mysql_field_size(mysql_type):
    """Stima la dimensione in bytes di un campo MySQL."""
    if mysql_type.startswith('VARCHAR'):
        # Estrai la dimensione dal VARCHAR
        size_str = mysql_type[8:-1]  # Rimuove 'VARCHAR(' e ')'
        return int(size_str) * 3  # UTF8MB4 usa max 3 bytes per carattere
    elif mysql_type == 'TEXT':
        return 768  # Dimensione minima per TEXT in InnoDB
    elif mysql_type == 'LONGTEXT':
        return 1024
    elif mysql_type in ['SMALLINT']:
        return 2
    elif mysql_type in ['INT']:
        return 4
    elif mysql_type in ['BIGINT']:
        return 8
    elif mysql_type.startswith('DECIMAL'):
        return 8  # Approssimazione per DECIMAL
    elif mysql_type in ['DATE']:
        return 3
    elif mysql_type in ['DATETIME', 'TIMESTAMP']:
        return 8
    elif mysql_type in ['TIME']:
        return 3
    elif mysql_type in ['BOOLEAN']:
        return 1
    elif mysql_type == 'JSON':
        return 0  # I campi JSON vanno in tabelle separate
    else:
        return 10  # Default per tipi sconosciuti

def check_memory_limit():
    """Controlla se abbiamo raggiunto il limite di memoria."""
    process = psutil.Process()
    current_memory = process.memory_info().rss
    max_memory_bytes = USABLE_MEMORY_BYTES * 0.9  # 90% del buffer disponibile per l'analisi
    return current_memory < max_memory_bytes

def generate_short_alias(field_name, existing_aliases):
    """Genera un alias corto per un campo lungo."""
    # Rimuovi caratteri speciali e spazi
    base = ''.join(c for c in field_name if c.isalnum())
    # Prendi le prime lettere di ogni parola
    words = base.split('_')
    if len(words) > 1:
        # Se ci sono pi� parole, usa le iniziali
        alias = ''.join(word[0] for word in words if word)
    else:
        # Altrimenti usa i primi caratteri
        alias = base[:8]
    
    # Aggiungi un numero se l'alias esiste gi�
    counter = 1
    original_alias = alias
    while alias in existing_aliases:
        alias = f"{original_alias}{counter}"
        counter += 1
    
    return alias

def sanitize_field_name(field_name, existing_aliases=None):
    """Sanitizza il nome del campo per MySQL."""
    if existing_aliases is None:
        existing_aliases = set()
    
    # Sostituisce i caratteri non validi con underscore
    sanitized = field_name.replace('-', '_')
    # Rimuove altri caratteri non validi
    sanitized = ''.join(c for c in sanitized if c.isalnum() or c == '_')
    
    # Se il nome � troppo lungo, genera un alias
    if len(sanitized) > 64:
        short_alias = generate_short_alias(sanitized, existing_aliases)
        existing_aliases.add(short_alias)
        return short_alias
    
    return sanitized

def get_column_type(field_type, length):
    """Determina il tipo di colonna appropriato in base alla lunghezza."""
    if field_type == 'VARCHAR':
        # Se la lunghezza � maggiore di 1000, usa TEXT
        if length > 1000:
            return 'TEXT'
        # Altrimenti usa VARCHAR con la lunghezza specificata
        return f'VARCHAR({length})'
    return field_type

def verify_table_structure(conn, table_name='main_data'):
    """Verifica la struttura della tabella creata."""
    with LogContext(db_logger, f"verifica struttura tabella {table_name}"):
        cursor = conn.cursor()
        try:
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            
            db_logger.info(f"Struttura tabella {table_name}:")
            date_columns = []
            for column in columns:
                field_name, field_type, is_null, key, default, extra = column
                db_logger.info(f"   [OK] {field_name}: {field_type}")
                
                # Evidenzia i campi data
                if 'data_' in field_name.lower() or field_name.lower() in ['created_at', 'updated_at']:
                    date_columns.append((field_name, field_type))
            
            if date_columns:
                db_logger.info(f"Campi data nella tabella:")
                for field_name, field_type in date_columns:
                    status = "[OK]" if field_type.upper() in ['DATE', 'DATETIME', 'TIMESTAMP'] else "[WARN]"
                    db_logger.info(f"   {status} {field_name}: {field_type}")
                    
        except mysql.connector.Error as e:
            log_error_with_context(db_logger, e, table_name, "verifica struttura")
        finally:
            cursor.close()

def create_main_table(cursor, table_definitions, field_mapping, column_types):
    """Crea la tabella principale con tutti i campi non-JSON."""
    with LogContext(db_logger, "creazione tabella principale"):
        # Escludi il campo 'cig' dalla lista dei campi normali poich� � gi� la chiave primaria
        main_fields = [f"{field_mapping[field]} {column_types[field]}" 
                      for field in table_definitions.keys() 
                      if field.lower() != 'cig']
        
        create_main_table = f"""
        CREATE TABLE IF NOT EXISTS main_data (
            cig VARCHAR(64) PRIMARY KEY,
            {', '.join(main_fields)},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file VARCHAR(255),
            batch_id VARCHAR(64),
            INDEX idx_created_at (created_at),
            INDEX idx_source_file (source_file),
            INDEX idx_batch_id (batch_id),
            INDEX idx_cig_source (cig, source_file),
            INDEX idx_cig_batch (cig, batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
        """
        
        db_logger.info(f"Creazione tabella main_data con {len(main_fields)} campi principali")
        cursor.execute(create_main_table)

def create_json_tables(cursor, table_definitions, field_mapping):
    """Crea le tabelle separate per i campi JSON."""
    json_tables_created = 0
    
    for field, def_type in table_definitions.items():
        if def_type == 'JSON':
            with LogContext(db_logger, f"creazione tabella JSON per campo {field}"):
                sanitized_field = field_mapping[field]
                create_json_table = f"""
                CREATE TABLE IF NOT EXISTS {sanitized_field}_data (
                    cig VARCHAR(64) PRIMARY KEY,
                    {sanitized_field}_json JSON,
                    source_file VARCHAR(255),
                    batch_id VARCHAR(64),
                    FOREIGN KEY (cig) REFERENCES main_data(cig),
                    INDEX idx_source_file (source_file),
                    INDEX idx_batch_id (batch_id),
                    INDEX idx_cig_source (cig, source_file),
                    INDEX idx_cig_batch (cig, batch_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
                """
                cursor.execute(create_json_table)
                json_tables_created += 1
    
    if json_tables_created > 0:
        db_logger.info(f"Tabelle JSON create: {json_tables_created}")
    
    return json_tables_created

def create_metadata_tables(cursor, table_definitions, field_mapping, column_types):
    """Crea le tabelle di metadati per tracking e mapping."""
    with LogContext(db_logger, "creazione tabelle metadati"):
        # Crea tabella per tracciare i file processati
        create_processed_files = """
        CREATE TABLE IF NOT EXISTS processed_files (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_name VARCHAR(255) UNIQUE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_count INT,
            status ENUM('completed', 'failed') DEFAULT 'completed',
            error_message TEXT,
            INDEX idx_file_name (file_name),
            INDEX idx_status (status),
            INDEX idx_processed_at (processed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
        """
        cursor.execute(create_processed_files)
        db_logger.info("Tabella processed_files creata")
        
        # Salva il mapping dei campi in una tabella di metadati
        create_field_mapping = """
        CREATE TABLE IF NOT EXISTS field_mapping (
            original_name VARCHAR(255) PRIMARY KEY,
            sanitized_name VARCHAR(64),
            field_type VARCHAR(50),
            INDEX idx_sanitized_name (sanitized_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
        """
        cursor.execute(create_field_mapping)
        db_logger.info("Tabella field_mapping creata")
        
        # Inserisci il mapping dei campi
        mapping_records = 0
        for field, def_type in table_definitions.items():
            cursor.execute("""
                INSERT INTO field_mapping (original_name, sanitized_name, field_type)
                VALUES (%s, %s, %s) AS new_mapping
                ON DUPLICATE KEY UPDATE
                    sanitized_name = new_mapping.sanitized_name,
                    field_type = new_mapping.field_type
            """, (field, field_mapping[field], column_types[field]))
            mapping_records += 1
        
        db_logger.info(f"Mapping campi inserito: {mapping_records} record")

def prepare_field_mappings(table_definitions):
    """Prepara i mapping dei campi e i tipi di colonna."""
    with LogContext(db_logger, "preparazione mapping campi", fields=len(table_definitions)):
        # Crea un mapping tra nomi originali e nomi sanitizzati
        existing_aliases = set()
        field_mapping = {}
        for field in table_definitions.keys():
            sanitized = sanitize_field_name(field, existing_aliases)
            field_mapping[field] = sanitized
            existing_aliases.add(sanitized)
        
        # Converti i tipi di colonna in base alla lunghezza
        column_types = {}
        for field, def_type in table_definitions.items():
            if def_type.startswith('VARCHAR'):
                # Estrai la lunghezza dal tipo VARCHAR
                length = int(def_type.split('(')[1].split(')')[0])
                column_types[field] = get_column_type('VARCHAR', length)
            else:
                column_types[field] = def_type
        
        db_logger.info(f"Field mapping preparato: {len(field_mapping)} campi")
        db_logger.info(f"Column types preparati: {len(column_types)} tipi")
        
        return field_mapping, column_types

def create_dynamic_tables(conn, table_definitions, categories):
    """Crea tutte le tabelle dinamiche necessarie per l'importazione per categoria."""
    with LogContext(db_logger, "creazione tabelle dinamiche", tables=len(categories), categories=len(categories)):
        cursor = conn.cursor()
        
        try:
            # 1. Prepara mapping dei campi e tipi di colonna
            field_mapping, column_types = prepare_field_mappings(table_definitions)
            
            # 2. Crea le tabelle per categoria (invece di una singola main_data)
            category_tables_count = create_category_tables(cursor, table_definitions, field_mapping, column_types, categories)
            
            # 3. Crea tabelle separate per i campi JSON
            json_tables_count = create_json_tables(cursor, table_definitions, field_mapping)
            
            # 4. Crea tabelle di metadati
            create_metadata_tables(cursor, table_definitions, field_mapping, column_types)
            
            db_logger.info(f"Creazione completata: {category_tables_count} tabelle per categoria, {json_tables_count} tabelle JSON, 2 tabelle metadati")
            
        finally:
            cursor.close()

def is_file_processed(conn, file_name):
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM processed_files WHERE file_name = %s", (file_name,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None and result[0] == 'completed'

def mark_file_processed(conn, file_name, record_count, status='completed', error_message=None):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO processed_files (file_name, record_count, status, error_message)
            VALUES (%s, %s, %s, %s) AS new_file
            ON DUPLICATE KEY UPDATE
                processed_at = CURRENT_TIMESTAMP,
                record_count = new_file.record_count,
                status = new_file.status,
                error_message = new_file.error_message
        """, (file_name, record_count, status, error_message))
        conn.commit()
    except Exception as e:
        log_error_with_context(db_logger, e, "mark file processed", f"file {file_name}")
        conn.rollback()
    finally:
        cursor.close()

def find_json_files(base_path):
    """Trova tutti i file JSON nella directory e sottodirectory usando pathlib."""
    base_dir = Path(base_path)
    
    if not base_dir.exists():
        import_logger.warning(f"Directory non trovata: {base_path}")
        return []
    
    if not base_dir.is_dir():
        import_logger.warning(f"Il percorso non è una directory: {base_path}")
        return []
    
    # Usa rglob per ricerca ricorsiva di tutti i file .json
    json_files = list(base_dir.rglob("*.json"))
    
    # Converte i Path objects in stringhe per compatibilità con il resto del codice
    json_file_paths = [str(json_file) for json_file in json_files]
    
    import_logger.info(f"Trovati {len(json_file_paths)} file JSON in {base_path}")
    return json_file_paths

# Mappa categorie → elenco di pattern regex da cercare nel nome file
CATEGORIES = {
    "aggiudicatari":          [r"aggiudicatari"],
    "aggiudicazioni":         [r"aggiudicazioni"],
    "avvio_contratto":        [r"avvio-contratto"],
    "categorie_dpcm":         [r"categorie-dpcm-aggregazione"],
    "categorie_opera":        [r"categorie-opera"],
    "cig":                    [r"^cig[_-]json", r"^smartcig[_-]json", r"cig[_-]json", r"smartcig[_-]json"],
    "collaudo":               [r"collaudo"],
    "fine_contratto":         [r"fine-contratto"],
    "fonti_finanziamento":    [r"fonti-finanziamento"],
    "lavorazioni":            [r"lavorazioni"],
    "partecipanti":           [r"partecipanti"],
    "pubblicazioni":          [r"pubblicazioni"],
    "quadro_economico":       [r"quadro-economico"],
    "sospensioni":            [r"sospensioni"],
    "stati_avanzamento":      [r"stati-avanzamento"],
    "subappalti":             [r"subappalti"],
    "varianti":               [r"varianti"],
    # categorie "extra" dai tuoi nomi:
    "bandi_modalita":         [r"bandi-cig-modalita-realizzazione"],
    "bandi_tipo_scelta":      [r"bandi-cig-tipo-scelta-contraente"],
    "centri_di_costo":        [r"centri-di-costo"],
    "pnrr_indicatori":        [r"indicatori-pnrrpnc"],
    "pnrr_misurepremiali":    [r"misurepremiali-pnrrpnc"],
    "stazioni_appaltanti":    [r"stazioni-appaltanti"],
    # NUOVE CATEGORIE per file non categorizzati:
    "cup":                    [r"cup[_-]json"],
    "smartcig_fattispecie":   [r"smartcig-tipo-fattispecie-contrattuale"],
}

def group_files_by_category(json_files):
    """
    Raggruppa i file JSON per categoria basata su pattern regex predefiniti.
    
    Ignora le date nel nome file (es. 20240201_, 20240401_) per raggruppare
    file dello stesso tipo ma di periodi diversi nella stessa tabella.
    """
    # Inizializza le categorie vuote
    categorized_files = {category: [] for category in CATEGORIES.keys()}
    uncategorized_files = []
    
    for json_file in json_files:
        file_path = Path(json_file)
        file_name = file_path.name  # Solo il nome del file, senza path
        
        # Rimuovi estensione per il matching
        file_stem = file_path.stem
        
        # RIMUOVI LE DATE dal nome file prima del pattern matching
        # Pattern per date: YYYYMMDD_ o YYYYMMDD- all'inizio (es. 20240201_, 20240401-, etc.)
        cleaned_name = re.sub(r'^\d{8}[_-]', '', file_stem)
        
        # Log del cleaning per debug
        if cleaned_name != file_stem:
            import_logger.debug(f"Cleaned file name: '{file_stem}' -> '{cleaned_name}'")
        
        category_found = False
        
        # Testa ogni categoria con i suoi pattern sul nome pulito
        for category, patterns in CATEGORIES.items():
            for pattern in patterns:
                # Cerca il pattern nel nome del file pulito (senza date)
                if re.search(pattern, cleaned_name, re.IGNORECASE):
                    categorized_files[category].append(json_file)
                    import_logger.debug(f"File '{file_name}' -> categoria '{category}' (pattern: '{pattern}' su nome pulito: '{cleaned_name}')")
                    category_found = True
                    break
            
            if category_found:
                break
        
        # Se nessun pattern ha fatto match, aggiungi agli uncategorized
        if not category_found:
            uncategorized_files.append(json_file)
            import_logger.warning(f"File NON categorizzato: '{file_name}' (nome pulito: '{cleaned_name}')")
    
    # Rimuovi categorie vuote dal risultato
    final_categories = {cat: files for cat, files in categorized_files.items() if files}
    
    # Statistiche di categorizzazione con info sul date cleaning
    total_categorized = sum(len(files) for files in final_categories.values())
    import_logger.info(f"[CATEGORIES] Categorizzazione completata (IGNORA DATE):")
    import_logger.info(f"  - File categorizzati: {total_categorized}")
    import_logger.info(f"  - File NON categorizzati: {len(uncategorized_files)}")
    import_logger.info(f"  - Categorie attive: {len(final_categories)}")
    import_logger.info(f"  - Date rimosse automaticamente da nomi file")
    
    # Report per categoria con raggruppamento per data
    for category, files in final_categories.items():
        # Raggruppa per data per mostrare quanti file per ogni periodo
        date_groups = defaultdict(int)
        for file_path in files:
            file_name = Path(file_path).name
            date_match = re.match(r'^(\d{8})_', file_name)
            if date_match:
                date = date_match.group(1)
                date_groups[date] += 1
            else:
                date_groups['no_date'] += 1
        
        dates_info = ', '.join([f"{date}({count})" for date, count in sorted(date_groups.items())])
        import_logger.info(f"    * {category}: {len(files)} file totali - Date: {dates_info}")
    
    if uncategorized_files:
        import_logger.warning(f"[WARN] File non categorizzati:")
        for uncategorized in uncategorized_files[:10]:  # Mostra solo i primi 10
            import_logger.warning(f"    - {Path(uncategorized).name}")
        if len(uncategorized_files) > 10:
            import_logger.warning(f"    ... e altri {len(uncategorized_files) - 10} file")
    
    return final_categories

def test_categorization(base_path):
    """
    Testa la categorizzazione dei file senza eseguire l'import.
    Utile per verificare che i pattern regex funzionino correttamente.
    """
    import_logger.info(f"[TEST] Testing categorizzazione files in {base_path}")
    
    json_files = find_json_files(base_path)
    if not json_files:
        import_logger.warning(f"[TEST] Nessun file JSON trovato in {base_path}")
        return
    
    import_logger.info(f"[TEST] Trovati {len(json_files)} file JSON totali")
    categories = group_files_by_category(json_files)
    
    # Report dettagliato per categoria
    import_logger.info(f"[TEST] Report dettagliato categorizzazione:")
    for category, files in categories.items():
        import_logger.info(f"[TEST] === CATEGORIA: {category.upper()} ({len(files)} file) ===")
        for file_path in files[:5]:  # Mostra solo i primi 5 per categoria
            file_name = Path(file_path).name
            import_logger.info(f"[TEST]   - {file_name}")
        if len(files) > 5:
            import_logger.info(f"[TEST]   ... e altri {len(files) - 5} file")
    
    return categories

def print_category_patterns():
    """Stampa tutti i pattern definiti per il debug."""
    import_logger.info("[DEBUG] Pattern regex per categoria:")
    for category, patterns in CATEGORIES.items():
        import_logger.info(f"  {category}:")
        for pattern in patterns:
            import_logger.info(f"    - {pattern}")

def process_chunk_unified(args, execution_mode="sequential"):
    """
    Processa un chunk con logica unificata per multiprocessing e sequential.
    
    Args:
        args: (chunk, file_name, batch_id, table_definitions, category)
        execution_mode: "multiprocessing" o "sequential"
    """
    chunk, file_name, batch_id, table_definitions, category = args
    
    # Logging specifico per modalità
    if execution_mode == "multiprocessing":
        print(f"[Multiprocessing] Processo PID={os.getpid()} elabora chunk di {len(chunk)} record del file {file_name} per categoria '{category}'")
    else:
        batch_logger.info(f"[Sequential] Elabora chunk di {len(chunk)} record del file {file_name} per categoria '{category}'")
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            # Usa DatabaseManager per ottenere connessione dal pool
            with DatabaseManager.get_pooled_connection() as db_manager:
                try:
                    # Chiamata corretta a process_batch
                    if hasattr(db_manager, 'connection'):
                        success = process_batch(db_manager.connection, chunk, table_definitions, batch_id, category=category)
                    else:
                        success = process_batch(db_manager, chunk, table_definitions, batch_id, category=category)
                    
                    db_manager.connection.commit()
                    return  # Successo
                except Exception as e:
                    error_msg = f"Errore nel processare chunk (tentativo {attempt + 1}/{max_retries}): {e}"
                    if execution_mode == "multiprocessing":
                        print(f"[ERROR] {error_msg}")  # In multiprocessing usa print per evitare conflitti logger
                    else:
                        log_error_with_context(batch_logger, e, "chunk processing", f"tentativo {attempt + 1}/{max_retries}")
                    
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponenziale
                    else:
                        raise
        except Exception as e:
            if attempt == max_retries - 1:
                final_error = f"Errore nel processare chunk dopo {max_retries} tentativi: {e}"
                if execution_mode == "multiprocessing":
                    print(f"[ERROR] {final_error}")
                else:
                    log_error_with_context(batch_logger, e, "chunk processing", f"fallimento finale dopo {max_retries} tentativi")
                raise

def process_chunk(args):
    """Wrapper per multiprocessing - usa la funzione unificata."""
    return process_chunk_unified(args, execution_mode="multiprocessing")

def process_chunk_sequential(args):
    """Wrapper per sequential processing - usa la funzione unificata."""
    return process_chunk_unified(args, execution_mode="sequential")

def import_all_json_files(base_path, conn):
    with LogContext(import_logger, "importazione completa JSON", base_path=base_path):
        json_files = find_json_files(base_path)
        
        # Raggruppa i file per categoria
        categories = group_files_by_category(json_files)
        total_files = len(json_files)
        
        import_logger.info(f"Trovati {total_files} file JSON da importare in {len(categories)} categorie")
        
        # Mostra la modalità di analisi schema selezionata
        import_logger.info(f"[SCHEMA] Modalità analisi: {SCHEMA_ANALYSIS_MODE.upper()}")
        import_logger.info(f"[SCHEMA] Parametri: {SCHEMA_MAX_ROWS_PER_FILE} righe/file, {SCHEMA_MAX_FILES_PER_CATEGORY} file/categoria, {SCHEMA_MAX_SAMPLES_PER_FIELD} campioni/campo")
        
        # Gestisci il caso di nessun file trovato
        if total_files == 0:
            import_logger.warning(f"[WARN] Nessun file JSON trovato in {base_path}")
            import_logger.info(f"[INFO] Controlla che la directory contenga file .json")
            import_logger.info(f"[INFO] Path utilizzato: {os.path.abspath(base_path)}")
            return  # Esce senza errore
        
        # Gestisci il caso di nessuna categoria trovata
        if len(categories) == 0:
            import_logger.warning(f"[WARN] Nessuna categoria riconosciuta dai {total_files} file trovati")
            import_logger.info(f"[INFO] I pattern regex potrebbero non matchare i nomi dei file")
            import_logger.info(f"[INFO] Usa modalità --test per verificare la categorizzazione")
            return  # Esce senza errore
        
        # Inizializza il tracker del progresso
        progress_tracker = ProgressTracker(total_files)
        
        # NUOVA LOGICA: Analisi per categoria invece di analisi globale
        import_logger.info("[SCHEMA] Utilizzo schema ottimizzato per categoria")
        import_logger.info("[SCHEMA] Ogni tabella avrà solo i campi presenti nei suoi JSON")
        
        # Crea un dizionario che mappa categoria -> definizioni tabella
        category_table_definitions = {}
        
        for category, category_files in categories.items():
            import_logger.info(f"[SCHEMA] Analizzando schema per categoria '{category}'")
            
            # Analizza solo i file di questa categoria
            table_definitions = analyze_single_category(category, category_files)
            category_table_definitions[category] = table_definitions
            
            import_logger.info(f"[SCHEMA] Categoria '{category}': {len(table_definitions)} campi specifici")
        
        # Crea le tabelle ottimizzate per categoria
        create_dynamic_tables_by_category(category_table_definitions)
        
        # Inizializza il pool di connessioni centralizzato
        DatabaseManager.initialize_pool(pool_size=2)  # Solo 2 connessioni per mono-processo
        
        # Importa la funzione aggiornata da utils e passa i parametri
        current_insert_batch = calculate_dynamic_insert_batch_size()
        log_resource_optimization(import_logger)
        
        # Processa categoria per categoria con schema specifico
        total_processed_files = 0
        total_processed_records = 0
        
        for category, category_files in categories.items():
            import_logger.info(f"[CATEGORIA] Processando categoria '{category}' con {len(category_files)} file")
            
            # Usa le definizioni specifiche per questa categoria
            current_table_definitions = category_table_definitions[category]
            
            # Processa i file di questa categoria CON MONITORING AVANZATO
            for idx, json_file in enumerate(category_files, 1):
                file_name = Path(json_file).name
                
                import_logger.info(f"📁 [FILE] {total_processed_files + 1}/{total_files}: {file_name}")
                
                # ANALISI PRELIMINARE PER DECIDERE IL METODO
                file_analysis = analyze_file_before_processing(json_file)
                
                # SCELTA STRATEGIA INTELLIGENTE (NUOVO SISTEMA DINAMICO)
                strategy = choose_optimal_processing_strategy(file_analysis)
                
                import_logger.info(f"🎯 [STRATEGY] Strategia selezionata: {strategy['strategy'].upper()}")
                import_logger.info(f"🎯 [DESCRIPTION] {strategy['description']}")
                import_logger.info(f"🎯 [BATCH] Batch iniziale: {strategy['batch_size']:,} record")
                import_logger.info(f"🎯 [RATIONALE] {strategy['rationale']}")
                
                success = False
                
                # ESEGUI LA STRATEGIA SCELTA (PRIORITÀ AL DYNAMIC STREAMING)
                if strategy['strategy'] == 'dynamic_streaming':
                    # NUOVO: Streaming dinamico con adattamento automatico RAM
                    success = process_file_streaming_with_custom_batch(
                        json_file, category, current_table_definitions, progress_tracker, strategy
                    )
                    
                elif strategy['strategy'] == 'memory_optimized':
                    # File piccoli: massima velocità con caricamento completo in memoria
                    success = process_file_memory_optimized(
                        json_file, category, current_table_definitions, progress_tracker, strategy
                    )
                    
                elif strategy['strategy'] == 'balanced_batching':
                    # File medi: bilanciamento tra velocità e memoria
                    success = process_file_balanced_batching(
                        json_file, category, current_table_definitions, progress_tracker, strategy
                    )
                    
                elif strategy['strategy'] in ['streaming_optimized', 'streaming_safe']:
                    # File grandi: streaming sicuro con batch fisso
                    success = process_file_streaming_with_custom_batch(
                        json_file, category, current_table_definitions, progress_tracker, strategy
                    )
                
                # FALLBACK UNIVERSALE in caso di errore (SEMPRE DYNAMIC STREAMING)
                if not success:
                    import_logger.warning(f"🔄 [UNIVERSAL-FALLBACK] Strategia {strategy['strategy']} fallita, uso dynamic streaming sicuro...")
                    
                    safe_strategy = {
                        'strategy': 'dynamic_streaming',
                        'batch_size': 1_000,  # Batch molto piccoli per massima sicurezza
                        'description': 'Dynamic streaming di emergenza con batch minimi',
                        'rationale': 'Fallback universale per errori'
                    }
                    
                    success = process_file_streaming_with_custom_batch(
                        json_file, category, current_table_definitions, progress_tracker, safe_strategy
                    )
                
                if success:
                    total_processed_files += 1
                    # I record sono già contati nel progress_tracker
                    total_processed_records = progress_tracker.total_records_processed
                    
                    import_logger.info(f"✅ [DONE] File {file_name} completato con successo")
                else:
                    import_logger.warning(f"⚠️  [FAILED] File {file_name} fallito definitivamente - continuo con il prossimo")
                
                # Log progresso ogni 10 file
                if (total_processed_files + 1) % 10 == 0:
                    log_memory_status(import_logger, f"OGNI 10 FILE - Completati: {total_processed_files}")
                
                # Garbage collection ogni 5 file per mantenere memoria pulita
                if (total_processed_files + 1) % 5 == 0:
                    gc.collect()
                    import_logger.debug(f"🧹 [GC] Garbage collection eseguita dopo {total_processed_files + 1} file")
        
        # Statistiche finali
        final_stats = progress_tracker.get_global_stats()
        total_time = final_stats['elapsed_time']
        
        import_logger.info("="*80)
        import_logger.info("[COMPLETE] IMPORTAZIONE COMPLETATA!")
        log_file_progress(import_logger, final_stats['processed_files'], final_stats['total_files'], "completati")
        log_performance_stats(import_logger, "Record totali", final_stats['total_records'], total_time)
        import_logger.info(f"[TIME] Tempo totale: {str(timedelta(seconds=int(total_time)))}")
        import_logger.info(f"[CATEGORIES] Categorie processate: {len(categories)}")
        import_logger.info("="*80)

def process_batch_parallel(batch, table_definitions, field_mapping, file_name, batch_id):
    """Implementazione completa per process_batch_parallel."""
    main_data = []
    json_data = defaultdict(list)
    
    for record_data in batch:
        # Gestisci sia il formato vecchio (record, file_name) che nuovo (record, file_name, category)
        if len(record_data) == 3:
            record, _, category = record_data
        else:
            record, _ = record_data
            category = None
            
        cig = record.get('cig', '')
        if not cig:
            continue
            
        # Processo base: aggiungi solo CIG e source info
        main_values = [cig]
        
        # Aggiungi valori per tutti i campi della definizione tabella
        for field, def_type in table_definitions.items():
            if field.lower() == 'cig':
                continue
            
            field_lower = field.lower().replace(' ', '_')
            value = record.get(field_lower)
            
            # Processo semplificato per i tipi base
            if def_type == 'JSON' and value is not None:
                json_data[field_mapping[field]].append((cig, json.dumps(value)))
                value = None
            elif value is not None:
                if def_type.startswith('VARCHAR'):
                    value = str(value)[:1000]  # Tronca se troppo lungo
                elif def_type in ['DATE', 'DATETIME']:
                    # Processo date semplificato
                    if isinstance(value, str) and value.strip():
                        # Mantieni solo se sembra una data valida
                        if any(c in value for c in ['-', '/', ':']):
                            pass  # Mantieni il valore
                        else:
                            value = None
                    else:
                        value = None
            
            main_values.append(value)
        
        # Aggiungi metadata
        main_values.extend([file_name, batch_id])
        main_data.append(tuple(main_values))
    
    return main_data, json_data

class ProgressTracker:
    """Traccia il progresso dell'importazione con ETA e statistiche dettagliate."""
    
    def __init__(self, total_files):
        self.total_files = total_files
        self.processed_files = 0
        self.total_records_processed = 0
        self.start_time = time.time()
        self.current_file_start_time = None
        self.current_file_name = None
        self.current_file_records = 0
        self.current_file_total_records = 0
        self.file_speeds = []  # Per calcolare velocit� media
        self.lock = threading.Lock()
    
    def start_file(self, file_name, total_records):
        """Inizia il tracking di un nuovo file."""
        with self.lock:
            self.current_file_name = file_name
            self.current_file_total_records = total_records
            self.current_file_records = 0
            self.current_file_start_time = time.time()
            
            elapsed_total = time.time() - self.start_time
            avg_speed = self.total_records_processed / elapsed_total if elapsed_total > 0 else 0
            
            progress_logger.info(f"File {self.processed_files + 1}/{self.total_files}: {file_name}")
            progress_logger.info(f"Record nel file: {total_records:,}")
            log_file_progress(progress_logger, self.processed_files, self.total_files, "globale")
            log_performance_stats(progress_logger, "Velocit� media", self.total_records_processed, elapsed_total)
    
    def update_file_progress(self, records_completed):
        """Aggiorna il progresso del file corrente."""
        with self.lock:
            self.current_file_records = records_completed
            
            if self.current_file_start_time:
                file_elapsed = time.time() - self.current_file_start_time
                file_speed = records_completed / file_elapsed if file_elapsed > 0 else 0
                file_progress = records_completed / self.current_file_total_records * 100 if self.current_file_total_records > 0 else 0
                
                # ETA per il file corrente
                if file_speed > 0:
                    remaining_records = self.current_file_total_records - records_completed
                    file_eta_seconds = remaining_records / file_speed
                    file_eta = str(timedelta(seconds=int(file_eta_seconds)))
                else:
                    file_eta = "N/A"
                
                log_batch_progress(progress_logger, records_completed, self.current_file_total_records, file_speed, f"ETA: {file_eta}")
    
    def finish_file(self, records_processed):
        """Completa il tracking del file corrente."""
        with self.lock:
            self.processed_files += 1
            self.total_records_processed += records_processed
            
            if self.current_file_start_time:
                file_time = time.time() - self.current_file_start_time
                file_speed = records_processed / file_time if file_time > 0 else 0
                self.file_speeds.append(file_speed)
                
                # Statistiche globali
                total_elapsed = time.time() - self.start_time
                global_avg_speed = self.total_records_processed / total_elapsed if total_elapsed > 0 else 0
                
                # ETA globale basato sui file rimanenti e velocit� media
                remaining_files = self.total_files - self.processed_files
                if len(self.file_speeds) > 0 and remaining_files > 0:
                    avg_file_speed = sum(self.file_speeds[-5:]) / len(self.file_speeds[-5:])  # Media ultime 5 velocit�
                    avg_records_per_file = self.total_records_processed / self.processed_files
                    estimated_remaining_records = remaining_files * avg_records_per_file
                    global_eta_seconds = estimated_remaining_records / avg_file_speed if avg_file_speed > 0 else 0
                    global_eta = str(timedelta(seconds=int(global_eta_seconds)))
                else:
                    global_eta = "N/A"
                
                progress_logger.info(f"? File completato: {self.current_file_name}")
                log_performance_stats(progress_logger, "Record processati", records_processed, file_time)
                log_file_progress(progress_logger, self.processed_files, self.total_files, "globale")
                log_performance_stats(progress_logger, "Record totali", self.total_records_processed, total_elapsed, "media globale")
                progress_logger.info(f"?? ETA completamento: {global_eta} ({remaining_files} file rimanenti)")
                
                # Reset file corrente
                self.current_file_name = None
                self.current_file_start_time = None
                self.current_file_records = 0
                self.current_file_total_records = 0
    
    def get_global_stats(self):
        """Restituisce statistiche globali del progresso."""
        with self.lock:
            total_elapsed = time.time() - self.start_time
            avg_speed = self.total_records_processed / total_elapsed if total_elapsed > 0 else 0
            completion_pct = self.processed_files / self.total_files * 100 if self.total_files > 0 else 0
            
            return {
                'processed_files': self.processed_files,
                'total_files': self.total_files,
                'completion_pct': completion_pct,
                'total_records': self.total_records_processed,
                'avg_speed': avg_speed,
                'elapsed_time': total_elapsed
            }

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Prominent log message to confirm script execution
    logger.info("🚀🚀🚀 [SCRIPT EXECUTION CONFIRMATION] Running updated src/import_json_mysql.py - Version 2025-06-04-11:00 - Check for InterfaceError handling 🚀🚀🚀")
    
    import sys
    
    logger.info("[VERSION CHECK] Executing updated src/import_json_mysql.py with InterfaceError handling - Version 2025-06-04-10:50")
    
    # Modalità test se viene passato il parametro --test
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        try:
            with LogContext(logger, "test categorizzazione"):
                logger.info(f"[TEST MODE] Inizio test categorizzazione: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Stampa i pattern per debug
                print_category_patterns()
                
                # Testa la categorizzazione
                categories = test_categorization(JSON_BASE_PATH)
                
                if categories:
                    logger.info(f"[TEST] [SUCCESS] Test completato! Trovate {len(categories)} categorie attive")
                    
                    # Mostra statistiche finali
                    total_files = sum(len(files) for files in categories.values())
                    logger.info(f"[TEST] [STATS] Statistiche finali:")
                    logger.info(f"[TEST]   - File totali categorizzati: {total_files}")
                    logger.info(f"[TEST]   - Categorie con file: {len(categories)}")
                    logger.info(f"[TEST]   - Categorie definite: {len(CATEGORIES)}")
                else:
                    logger.warning(f"[TEST] [WARN] Nessuna categoria trovata!")
                
        except Exception as e:
            log_error_with_context(logger, e, "test categorizzazione")
            raise
        
        return  # Esce senza fare l'importazione
    
    # Modalità cleanup se viene passato il parametro --cleanup
    if len(sys.argv) > 1 and sys.argv[1] == '--cleanup':
        try:
            with LogContext(logger, "pulizia tabelle problematiche"):
                logger.info(f"[CLEANUP MODE] Inizio pulizia: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Trova le categorie esistenti
                json_files = find_json_files(JSON_BASE_PATH)
                categories = group_files_by_category(json_files)
                
                with DatabaseManager.get_connection() as conn:
                    clean_problematic_tables(conn, categories.keys())
                
                logger.info(f"[CLEANUP] Pulizia completata!")
                
        except Exception as e:
            log_error_with_context(logger, e, "pulizia tabelle")
            raise
        
        return  # Esce dopo la pulizia
    
    # Modalità normale (importazione completa)
    try:
        with LogContext(logger, "importazione MySQL"):
            logger.info(f"Inizio importazione: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            log_memory_status(memory_logger, "inizio")
            logger.info(f"Chunk size iniziale calcolato: {INITIAL_CHUNK_SIZE}")
            logger.info(f"Chunk size massimo calcolato: {MAX_CHUNK_SIZE}")
        
        # Verifica lo spazio disco
        check_disk_space()
        
        # Pulizia preventiva automatica delle tabelle problematiche
        try:
            json_files = find_json_files(JSON_BASE_PATH)
            categories = group_files_by_category(json_files)
            
            logger.info("[AUTO-CLEANUP] Verifica e pulizia preventiva tabelle...")
            with DatabaseManager.get_connection() as conn:
                clean_problematic_tables(conn, categories.keys())
        except Exception as cleanup_error:
            logger.warning(f"[AUTO-CLEANUP] Errore durante pulizia preventiva: {cleanup_error}")
            # Non bloccare l'importazione per errori di pulizia
        
        # Usa DatabaseManager per la connessione principale
        with DatabaseManager.get_connection() as conn:
            import_all_json_files(JSON_BASE_PATH, conn)
            
        # Chiudi il pool alla fine
        DatabaseManager.close_pool()
        logger.info("Tutte le connessioni chiuse.")
    except Exception as e:
        actual_error_type_name = type(e).__name__
        is_interface_error = (
            actual_error_type_name == 'MySQLInterfaceError' or
            actual_error_type_name == 'InterfaceError' or
            hasattr(e, '__module__') and e.__module__.startswith('mysql.connector') and 'InterfaceError' in actual_error_type_name
        )
        logger.info(f"[DEBUG main()] Caught exception in main(). Type: '{actual_error_type_name}', Detected as InterfaceError: {is_interface_error}")
        if is_interface_error:
            errno = getattr(e, 'errno', 'N/A')
            sqlstate = getattr(e, 'sqlstate', 'N/A')
            safe_error_message = f"A MySQL Interface Error occurred (type: {actual_error_type_name}, errno: {errno}, sqlstate: {sqlstate}). Operation failed. Original message suppressed."
            logger.error(f"[ERROR] [main] Errore durante importazione MySQL: {safe_error_message}")
        else:
            logger.error(f"[ERROR] [main] Errore durante importazione MySQL: {e}")
        raise

def create_category_tables(cursor, table_definitions, field_mapping, column_types, categories):
    """Crea le tabelle per categoria invece di una singola tabella principale."""
    with LogContext(db_logger, "creazione tabelle per categoria", categories=len(categories)):
        
        tables_created = 0
        
        for category in categories.keys():
            # Nome della tabella per questa categoria
            table_name = f"{category}_data"
            
            # Escludi il campo 'cig' dalla lista dei campi normali poiché è già la chiave primaria
            main_fields = [f"{field_mapping[field]} {column_types[field]}" 
                          for field in table_definitions.keys() 
                          if field.lower() != 'cig']
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                cig VARCHAR(64) NOT NULL,
                {', '.join(main_fields)},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_file VARCHAR(255),
                batch_id VARCHAR(64),
                INDEX idx_cig (cig),
                INDEX idx_created_at (created_at),
                INDEX idx_source_file (source_file),
                INDEX idx_batch_id (batch_id),
                INDEX idx_cig_source (cig, source_file),
                INDEX idx_cig_batch (cig, batch_id),
                INDEX idx_cig_source_batch (cig, source_file, batch_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
            """
            
            db_logger.info(f"Creazione tabella {table_name} con {len(main_fields)} campi principali")
            cursor.execute(create_table_sql)
            tables_created += 1
        
        db_logger.info(f"Create {tables_created} tabelle per categoria")
        return tables_created

# FUNZIONE RIMOSSA: analyze_json_structure_by_category 
# Non viene mai chiamata e contiene bug dei prefissi categoria
# Il flusso principale usa analyze_single_category direttamente

def analyze_single_category(category, json_files):
    """Analizza la struttura JSON per una singola categoria."""
    from collections import defaultdict, Counter
    import statistics
    
    # Strutture dati per analisi approfondita
    field_types = defaultdict(lambda: defaultdict(int))
    field_lengths = defaultdict(list)  # Lista delle lunghezze per statistiche
    field_sample_values = defaultdict(list)
    field_null_counts = defaultdict(int)
    field_unique_counts = defaultdict(set)
    field_numeric_stats = defaultdict(list)  # Per valori numerici
    
    # Statistiche per categoria
    category_stats = {
        'total_files_analyzed': 0,
        'total_records_analyzed': 0,
        'files_with_errors': 0,
        'files_skipped': 0,
        'json_parse_errors': 0,
        'encoding_errors': 0,
        'unique_fields_found': set(),
        'common_field_patterns': defaultdict(int)
    }
    
    # USA PARAMETRI CONFIGURABILI PER VELOCITÀ
    MAX_ROWS_PER_FILE = min(100, SCHEMA_MAX_ROWS_PER_FILE)  # Massimo 100 come richiesto
    MAX_FILES_PER_CATEGORY = SCHEMA_MAX_FILES_PER_CATEGORY
    MAX_SAMPLES_PER_FIELD = SCHEMA_MAX_SAMPLES_PER_FIELD
    
    process = psutil.Process()
    max_memory_bytes = USABLE_MEMORY_BYTES * 0.9
    
    # Seleziona un sottoinsieme di file se ce ne sono troppi
    files_to_analyze = json_files
    if len(json_files) > MAX_FILES_PER_CATEGORY:
        # Prendi file distribuiti uniformemente attraverso la lista
        step = max(1, len(json_files) // MAX_FILES_PER_CATEGORY)
        files_to_analyze = [json_files[i] for i in range(0, len(json_files), step)][:MAX_FILES_PER_CATEGORY]
        analysis_logger.info(f"  [SAMPLE] Categoria '{category}': campiono {len(files_to_analyze)} file su {len(json_files)} totali")
    
    analysis_logger.info(f"  [ANALISI] Categoria '{category}' [{SCHEMA_ANALYSIS_MODE.upper()}]: {len(files_to_analyze)} file, max {MAX_ROWS_PER_FILE} righe/file")
    
    # Pattern avanzati per identificare i tipi
    patterns = {
        # Tipi numerici precisi
        'pure_integer': r'^\d+$',
        'negative_integer': r'^-\d+$',
        'pure_decimal': r'^\d+[.,]\d+$',
        'negative_decimal': r'^-\d+[.,]\d+$',
        'scientific': r'^\d+[.,]?\d*[eE][+-]?\d+$',
        'zero': r'^0+$',
        
        # Valori monetari e percentuali
        'monetary_euro': r'^€?\s*\d+([.,]\d{1,2})?$',
        'monetary_dollar': r'^\$?\s*\d+([.,]\d{1,2})?$',
        'percentage': r'^\d+([.,]\d+)?%$',
        'negative_monetary': r'^-€?\$?\s*\d+([.,]\d{1,2})?$',
        
        # Date e tempi (più pattern)
        'date_iso': r'^\d{4}-\d{2}-\d{2}$',
        'date_iso_extended': r'^\d{4}-\d{1,2}-\d{1,2}$',
        'date_european': r'^\d{2}/\d{2}/\d{4}$',
        'date_american': r'^\d{2}/\d{2}/\d{4}$',
        'date_italian': r'^\d{2}-\d{2}-\d{4}$',
        'date_dot_format': r'^\d{2}\.\d{2}\.\d{4}$',
        'datetime_iso': r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',
        'datetime_european': r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',
        'time_hhmm': r'^\d{2}:\d{2}$',
        'time_hhmmss': r'^\d{2}:\d{2}:\d{2}$',
        'timestamp_unix': r'^\d{10,13}$',
        'year_4digit': r'^(19|20)\d{2}$',
        'month_name_it': r'^(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)$',
        'month_name_en': r'^(january|february|march|april|may|june|july|august|september|october|november|december)$',
        
        # Booleani (più varianti)
        'boolean_it': r'^(si|no|vero|falso|sì)$',
        'boolean_en': r'^(true|false|yes|no|y|n)$',
        'boolean_numeric': r'^[01]$',
        'boolean_ticks': r'^[✓✗×]$',
        
        # Codici specifici italiani
        'fiscal_code': r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$',
        'partita_iva': r'^\d{11}$',
        'cup_code': r'^[A-Z]\d{13}$',
        'cig_code': r'^[A-Z0-9]{10}$',
        'cig_code_extended': r'^[A-Z0-9]{8,15}$',
        'postal_code_it': r'^\d{5}$',
        'istat_code': r'^\d{6,9}$',
        'rea_number': r'^[A-Z]{2}-\d{6,7}$',
        
        # Email, URL, telefoni
        'email': r'^[^@\s]+@[^@\s]+\.[^@\s]+$',
        'url_http': r'^https?://[^\s]+$',
        'url_www': r'^www\.[^\s]+$',
        'phone_it': r'^(\+39\s?)?(\d{2,4}[\s.-]?)?\d{6,10}$',
        'phone_international': r'^\+\d{1,3}\s?\d{4,14}$',
        
        # Alfanumerici e codici
        'alphanumeric_upper': r'^[A-Z0-9]+$',
        'alphanumeric_mixed': r'^[A-Za-z0-9]+$',
        'code_with_dashes': r'^[A-Z0-9-]+$',
        'code_with_slashes': r'^[A-Z0-9/]+$',
        'uuid_format': r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',
        'guid_format': r'^[{]?[0-9a-fA-F]{8}[-]?([0-9a-fA-F]{4}[-]?){3}[0-9a-fA-F]{12}[}]?$',
        
        # Indirizzi
        'address_pattern': r'^(via|viale|piazza|corso|largo|strada|località|loc\.|v\.le|p\.za)\s+',
        'cap_and_city': r'^\d{5}\s+[A-Za-z\s]+$',
        
        # Testi strutturati
        'text_all_caps': r'^[A-Z\s,.-]+$',
        'text_title_case': r'^[A-Z][a-z]+(\s[A-Z][a-z]+)*$',
        'text_with_numbers': r'^.*\d.*$',
        'text_short': r'^.{1,50}$',
        'text_medium': r'^.{51,255}$',
        'text_long': r'^.{256,1000}$',
        'text_very_long': r'^.{1001,}$',
        
        # Pattern JSON/XML
        'json_array': r'^\[.*\]$',
        'json_object': r'^{.*}$',
        'xml_tag': r'^<[^>]+>.*</[^>]+>$',
        
        # Valori speciali
        'empty_string': r'^$',
        'whitespace_only': r'^\s+$',
        'null_string': r'^(null|NULL|nil|NIL|none|NONE)$',
        'not_available': r'^(n/a|N/A|na|NA|non disponibile|nd|ND)$'
    }
    
    def analyze_value_type_advanced(value):
        """Analizza un singolo valore con logica avanzata."""
        if value is None:
            return 'null'
        
        if isinstance(value, bool):
            return 'boolean_native'
        if isinstance(value, int):
            return 'integer_native'
        if isinstance(value, float):
            return 'decimal_native'
        if isinstance(value, (list, dict)):
            return 'json_native'
            
        value_str = str(value).strip()
        if not value_str:
            return 'empty'
        
        # PRIORITÀ ALTA: Pattern specifici prima dei generici
        high_priority_patterns = [
            'fiscal_code', 'cig_code', 'cig_code_extended', 'cup_code', 'partita_iva', 
            'uuid_format', 'guid_format', 'email', 'url_http', 'url_www', 
            'phone_it', 'phone_international', 'postal_code_it', 'istat_code', 'rea_number'
        ]
        
        # PRIORITÀ MEDIA: Date e booleani
        medium_priority_patterns = [
            'datetime_iso', 'datetime_european', 'date_iso', 'date_iso_extended',
            'date_european', 'date_american', 'date_italian', 'date_dot_format',
            'boolean_it', 'boolean_en', 'boolean_numeric', 'boolean_ticks',
            'timestamp_unix', 'year_4digit'
        ]
        
        # PRIORITÀ BASSA: Pattern generici
        low_priority_patterns = [
            'monetary_euro', 'monetary_dollar', 'negative_monetary', 'percentage',
            'pure_integer', 'negative_integer', 'pure_decimal', 'negative_decimal',
            'scientific', 'zero'
        ]
        
        # Testa i pattern in ordine di priorità
        for pattern_list in [high_priority_patterns, medium_priority_patterns, low_priority_patterns]:
            for pattern_name in pattern_list:
                if pattern_name in patterns:
                    try:
                        if re.match(patterns[pattern_name], value_str, re.IGNORECASE):
                            return pattern_name
                    except re.error:
                        continue
        
        # Pattern testuali e lunghi (ultima priorità)
        try:
            length = len(value_str)
            
            # Controlla pattern speciali
            for pattern_name in ['json_array', 'json_object', 'xml_tag', 'address_pattern', 
                               'cap_and_city', 'text_all_caps', 'text_title_case', 'text_with_numbers',
                               'month_name_it', 'month_name_en', 'null_string', 'not_available']:
                if pattern_name in patterns:
                    if re.match(patterns[pattern_name], value_str, re.IGNORECASE):
                        return pattern_name
            
            # Classificazione per lunghezza
            if length <= 50:
                return 'text_short'
            elif length <= 255:
                return 'text_medium'
            elif length <= 1000:
                return 'text_long'
            else:
                return 'text_very_long'
                
        except Exception:
            return 'text_unknown'
    
    def determine_mysql_type_advanced(field, type_counts, length_stats, sample_values, numeric_stats):
        """Determina il tipo MySQL con logica avanzata basata su statistiche."""
        total_values = sum(type_counts.values())
        if total_values == 0:
            return 'VARCHAR(50)'
        
        # Calcola statistiche sui tipi
        sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
        primary_type = sorted_types[0][0]
        primary_count = sorted_types[0][1]
        primary_percentage = (primary_count / total_values) * 100
        
        # Calcola statistiche sulle lunghezze
        max_length = max(length_stats) if length_stats else 0
        avg_length = statistics.mean(length_stats) if length_stats else 0
        median_length = statistics.median(length_stats) if length_stats else 0
        
        # Log dettagliato per debug
        analysis_logger.debug(f"    Campo '{field}': tipo primario='{primary_type}' ({primary_percentage:.1f}%), "
                             f"lunghezza max={max_length}, media={avg_length:.1f}, mediana={median_length}")
        
        # Decisione tipo MySQL con soglie più conservative
        if primary_percentage >= 85:  # Soglia più alta per maggiore sicurezza
            # Tipi numerici
            if primary_type in ['integer_native', 'pure_integer', 'negative_integer']:
                if numeric_stats:
                    max_val = max(numeric_stats)
                    min_val = min(numeric_stats)
                    if min_val >= 0 and max_val <= 255:
                        return 'TINYINT UNSIGNED'
                    elif min_val >= -128 and max_val <= 127:
                        return 'TINYINT'
                    elif min_val >= 0 and max_val <= 65535:
                        return 'SMALLINT UNSIGNED'
                    elif min_val >= -32768 and max_val <= 32767:
                        return 'SMALLINT'
                    elif min_val >= 0 and max_val <= 4294967295:
                        return 'INT UNSIGNED'
                    elif min_val >= -2147483648 and max_val <= 2147483647:
                        return 'INT'
                    else:
                        return 'BIGINT'
                return 'INT'
                
            elif primary_type in ['decimal_native', 'pure_decimal', 'negative_decimal']:
                if numeric_stats:
                    # Calcola precisione necessaria
                    max_precision = 10  # default
                    max_scale = 2      # default
                    for val in sample_values[:10]:  # Controlla campioni
                        if isinstance(val, (int, float)):
                            val_str = str(val)
                            if '.' in val_str:
                                parts = val_str.split('.')
                                max_precision = max(max_precision, len(parts[0]) + len(parts[1]))
                                max_scale = max(max_scale, len(parts[1]))
                    return f'DECIMAL({min(max_precision, 20)},{min(max_scale, 6)})'
                return 'DECIMAL(20,6)'
                
            elif primary_type in ['monetary_euro', 'monetary_dollar']:
                return 'DECIMAL(15,2)'
            elif primary_type == 'percentage':
                return 'DECIMAL(5,2)'
                
            # Date e tempi
            elif primary_type in ['date_iso', 'date_iso_extended', 'date_european', 'date_american', 'date_italian', 'date_dot_format']:
                return 'DATE'
            elif primary_type in ['datetime_iso', 'datetime_european', 'timestamp_unix']:
                return 'DATETIME'
            elif primary_type in ['time_hhmm', 'time_hhmmss']:
                return 'TIME'
            elif primary_type == 'year_4digit':
                return 'YEAR'
                
            # Booleani
            elif primary_type in ['boolean_native', 'boolean_it', 'boolean_en', 'boolean_numeric', 'boolean_ticks']:
                return 'BOOLEAN'
                
            # Codici specifici
            elif primary_type == 'fiscal_code':
                return 'CHAR(16)'
            elif primary_type == 'partita_iva':
                return 'CHAR(11)'
            elif primary_type == 'cup_code':
                return 'CHAR(15)'
            elif primary_type in ['cig_code', 'cig_code_extended']:
                return f'VARCHAR({max_length + 5})'  # Buffer di sicurezza
            elif primary_type == 'postal_code_it':
                return 'CHAR(5)'
            elif primary_type == 'istat_code':
                return f'VARCHAR({max_length + 2})'
            elif primary_type in ['uuid_format', 'guid_format']:
                return 'CHAR(36)'
                
            # Email e URL
            elif primary_type == 'email':
                return f'VARCHAR({min(max(max_length, 100), 320)})'  # RFC limit è 320
            elif primary_type in ['url_http', 'url_www']:
                return 'TEXT'
            elif primary_type in ['phone_it', 'phone_international']:
                return f'VARCHAR({max(max_length, 20)})'
                
            # JSON
            elif primary_type in ['json_native', 'json_array', 'json_object']:
                return 'JSON'
            elif primary_type == 'xml_tag':
                return 'TEXT'
                
        # Fallback intelligente basato sulla lunghezza e pattern secondari
        if max_length == 0:
            return 'VARCHAR(50)'
        elif max_length <= 5 and primary_type in ['pure_integer', 'alphanumeric_upper']:
            return f'CHAR({max_length})'
        elif max_length <= 50:
            return f'VARCHAR({max_length + 10})'  # Buffer 20%
        elif max_length <= 255:
            return f'VARCHAR({max_length + 50})'   # Buffer fisso
        elif max_length <= 1000:
            return 'TEXT'
        else:
            return 'LONGTEXT'
    
    # ANALISI DEI FILE
    for json_file in files_to_analyze:
        category_stats['total_files_analyzed'] += 1
        file_records = 0
        file_has_errors = False
        
        try:
            # Prova diverse codifiche
            encodings_to_try = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
            file_content = None
            
            for encoding in encodings_to_try:
                try:
                    with open(json_file, 'r', encoding=encoding) as f:
                        file_content = f.readlines()
                    break
                except UnicodeDecodeError:
                    continue
            
            if file_content is None:
                analysis_logger.warning(f"  [ENCODING] Impossibile decodificare {json_file}")
                category_stats['encoding_errors'] += 1
                file_has_errors = True
                continue
            
            # Analizza le righe del file
            for line_num, line in enumerate(file_content, 1):
                if file_records >= MAX_ROWS_PER_FILE:
                    break
                    
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    file_records += 1
                    category_stats['total_records_analyzed'] += 1
                    
                    # Analizza ogni campo del record
                    for field, value in record.items():
                        original_field = field
                        field = field.lower().replace(' ', '_').replace('-', '_')
                        
                        # Aggiungi a statistiche globali
                        category_stats['unique_fields_found'].add(field)
                        
                        # Analisi del tipo
                        value_type = analyze_value_type_advanced(value)
                        field_types[field][value_type] += 1
                        
                        # Statistiche sui null
                        if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                            field_null_counts[field] += 1
                        else:
                            # Valori unici (campiona solo alcuni per performance)
                            if len(field_unique_counts[field]) < 1000:
                                field_unique_counts[field].add(str(value)[:100])  # Tronca a 100 char
                        
                        # Campioni di valori
                        if len(field_sample_values[field]) < MAX_SAMPLES_PER_FIELD:
                            field_sample_values[field].append(value)
                        
                        # Statistiche lunghezza
                        if isinstance(value, str):
                            current_length = len(value)
                            field_lengths[field].append(current_length)
                        
                        # Statistiche numeriche
                        if isinstance(value, (int, float)) and not isinstance(value, bool):
                            field_numeric_stats[field].append(value)
                        elif isinstance(value, str) and value.strip():
                            # Prova a convertire stringhe numeriche
                            try:
                                # Rimuovi caratteri di valuta e spazi
                                clean_value = re.sub(r'[€$,\s%]', '', value.replace(',', '.'))
                                if clean_value.replace('.', '').replace('-', '').isdigit():
                                    numeric_val = float(clean_value)
                                    field_numeric_stats[field].append(numeric_val)
                            except (ValueError, AttributeError):
                                pass
                        
                        # Pattern comuni del campo
                        if value_type not in ['null', 'empty']:
                            category_stats['common_field_patterns'][f"{field}:{value_type}"] += 1
                    
                    # Controllo memoria ogni 250 record
                    if file_records % 250 == 0:
                        current_memory = process.memory_info().rss
                        if current_memory > max_memory_bytes:
                            analysis_logger.warning(f"  [MEMORY] Limite memoria raggiunto, interrompo analisi file")
                            break
                            
                except json.JSONDecodeError as e:
                    category_stats['json_parse_errors'] += 1
                    if category_stats['json_parse_errors'] <= 5:  # Log solo i primi 5 errori
                        analysis_logger.warning(f"  [JSON] Errore parsing riga {line_num} in {json_file}: {e}")
                    file_has_errors = True
                    continue
                except Exception as e:
                    analysis_logger.warning(f"  [ERROR] Errore inaspettato riga {line_num} in {json_file}: {e}")
                    file_has_errors = True
                    continue
                        
        except Exception as e:
            analysis_logger.warning(f"  [FILE] Errore nell'apertura di {json_file}: {e}")
            category_stats['files_with_errors'] += 1
            continue
        
        if file_has_errors:
            category_stats['files_with_errors'] += 1
    
    # CREAZIONE DEFINIZIONI TABELLA
    table_definitions = {}
    for field, type_counts in field_types.items():
        if not type_counts:  # Skip campi senza dati
            continue
            
        mysql_type = determine_mysql_type_advanced(
            field, 
            type_counts, 
            field_lengths.get(field, [0]), 
            field_sample_values.get(field, []),
            field_numeric_stats.get(field, [])
        )
        table_definitions[field] = mysql_type
    
    # REPORT FINALE DETTAGLIATO
    null_percentage_high = [f for f in field_null_counts if 
                           field_null_counts[f] / max(sum(field_types[f].values()), 1) > 0.8]
    
    analysis_logger.info(f"  [STATS] Categoria '{category}': {category_stats['total_records_analyzed']:,} record analizzati "
                        f"da {category_stats['total_files_analyzed']} file")
    analysis_logger.info(f"  [QUALITY] Errori: {category_stats['files_with_errors']} file, "
                        f"{category_stats['json_parse_errors']} JSON malformati, "
                        f"{category_stats['encoding_errors']} errori encoding")
    analysis_logger.info(f"  [SCHEMA] Trovati {len(table_definitions)} campi unici")
    
    if null_percentage_high:
        analysis_logger.warning(f"  [NULL-WARN] Campi con >80% null: {', '.join(null_percentage_high[:5])}")
    
    # Log dei campi più interessanti
    interesting_fields = [f for f, types in field_types.items() 
                         if len(types) == 1 and list(types.keys())[0] in 
                         ['fiscal_code', 'cig_code', 'email', 'url_http', 'date_iso']]
    if interesting_fields:
        analysis_logger.info(f"  [PATTERNS] Campi tipizzati: {', '.join(interesting_fields[:10])}")
    
    return table_definitions

def create_dynamic_tables_by_category(category_table_definitions):
    """Crea tabelle dinamiche ottimizzate con schema specifico per ogni categoria usando connessioni robuste."""
    with LogContext(db_logger, "creazione tabelle ottimizzate per categoria", categories=len(category_table_definitions)):
        
        # Usa il DatabaseManager con riconnessione automatica
        with DatabaseManager.get_connection() as db_manager:
            total_tables_created = 0
            total_fields_created = 0
            
            try:
                # Prima verifica e migra le tabelle esistenti se necessario
                categories = list(category_table_definitions.keys())
                for category in categories:
                    table_name = f"{category}_data"
                    check_and_update_table_structure_robust(db_manager, table_name, {category: True})
                
                for category, table_definitions in category_table_definitions.items():
                    if not table_definitions:
                        db_logger.warning(f"[SKIP] Categoria '{category}' senza campi, skippo creazione tabella")
                        continue
                    
                    # Prepara mapping dei campi specifici per questa categoria
                    field_mapping, column_types = prepare_field_mappings(table_definitions)
                    
                    # Nome della tabella per questa categoria
                    table_name = f"{category}_data"
                    
                    # Campi principali (escludi cig che è la chiave primaria)
                    main_fields = [f"{field_mapping[field]} {column_types[field]}" 
                                  for field in table_definitions.keys() 
                                  if field.lower() != 'cig']
                    
                    # Crea la tabella ottimizzata con riconnessione automatica
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        cig VARCHAR(64) NOT NULL,
                        {', '.join(main_fields)},
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        source_file VARCHAR(255),
                        batch_id VARCHAR(64),
                        INDEX idx_cig (cig),
                        INDEX idx_created_at (created_at),
                        INDEX idx_source_file (source_file),
                        INDEX idx_batch_id (batch_id),
                        INDEX idx_cig_source (cig, source_file),
                        INDEX idx_cig_batch (cig, batch_id),
                        INDEX idx_cig_source_batch (cig, source_file, batch_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
                    """
                    
                    db_logger.info(f"[CREATE] Tabella ottimizzata '{table_name}' con {len(main_fields)} campi specifici")
                    
                    # Usa execute_with_retry per operazioni robuste
                    db_manager.execute_with_retry(create_table_sql)
                    total_tables_created += 1
                    total_fields_created += len(main_fields)
                    
                    # Crea tabelle JSON separate per campi JSON di questa categoria
                    json_tables_count = 0
                    for field, def_type in table_definitions.items():
                        if def_type == 'JSON':
                            sanitized_field = field_mapping[field]
                            json_table_name = f"{category}_{sanitized_field}_data"
                            
                            create_json_table = f"""
                            CREATE TABLE IF NOT EXISTS {json_table_name} (
                                cig VARCHAR(64) PRIMARY KEY,
                                {sanitized_field}_json JSON,
                                source_file VARCHAR(255),
                                batch_id VARCHAR(64),
                                INDEX idx_source_file (source_file),
                                INDEX idx_batch_id (batch_id),
                                INDEX idx_cig_source (cig, source_file),
                                INDEX idx_cig_batch (cig, batch_id)
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
                            """
                            
                            db_manager.execute_with_retry(create_json_table)
                            json_tables_count += 1
                    
                    if json_tables_count > 0:
                        db_logger.info(f"[CREATE] {json_tables_count} tabelle JSON specifiche per categoria '{category}'")
                    
                    # Report per categoria
                    db_logger.info(f"[RESULT] Categoria '{category}': tabella ottimizzata con {len(table_definitions)} campi totali")
                
                # Crea tabelle metadati globali con riconnessione automatica
                create_metadata_tables_robust(db_manager, category_table_definitions)
                
                # VERIFICA che le tabelle metadati esistano e contengano dati
                try:
                    mapping_count = db_manager.execute_with_retry("SELECT COUNT(*) FROM category_field_mapping", fetch=True)[0][0]
                    db_logger.info(f"[VERIFY] Tabella category_field_mapping contiene {mapping_count} record")
                    
                    data_tables = db_manager.execute_with_retry("SHOW TABLES LIKE '%_data'", fetch=True)
                    db_logger.info(f"[VERIFY] Trovate {len(data_tables)} tabelle dati: {[table[0] for table in data_tables]}")
                    
                    if mapping_count == 0:
                        db_logger.warning("[WARN] La tabella category_field_mapping è vuota!")
                    else:
                        db_logger.info("[VERIFY] Verifica completata con successo")
                        
                except Exception as verify_error:
                    db_logger.error(f"[ERROR] Errore durante verifica tabelle: {verify_error}")
                
                db_logger.info(f"[SUMMARY] Create {total_tables_created} tabelle ottimizzate con {total_fields_created} campi totali")
                db_logger.info(f"[SUMMARY] Schema ottimizzato: ogni tabella contiene solo i campi necessari")
                
            except Exception as e:
                db_logger.error(f"[ERROR] Errore durante creazione tabelle: {e}")
                raise

def create_metadata_tables_optimized(cursor, category_table_definitions):
    """Crea tabelle metadati ottimizzate per il sistema per categoria."""
    with LogContext(db_logger, "creazione tabelle metadati ottimizzate"):
        
        # Crea tabella per tracciare i file processati (invariata)
        create_processed_files = """
        CREATE TABLE IF NOT EXISTS processed_files (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_name VARCHAR(255) UNIQUE,
            category VARCHAR(100),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_count INT,
            status ENUM('completed', 'failed') DEFAULT 'completed',
            error_message TEXT,
            INDEX idx_file_name (file_name),
            INDEX idx_category (category),
            INDEX idx_status (status),
            INDEX idx_processed_at (processed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
        """
        cursor.execute(create_processed_files)
        db_logger.info("[CREATE] Tabella processed_files ottimizzata creata")
        
        # Crea tabella per mapping campi per categoria
        create_category_field_mapping = """
        CREATE TABLE IF NOT EXISTS category_field_mapping (
            id INT AUTO_INCREMENT PRIMARY KEY,
            category VARCHAR(100),
            original_name VARCHAR(255),
            sanitized_name VARCHAR(64),
            field_type VARCHAR(50),
            table_name VARCHAR(100),
            INDEX idx_category (category),
            INDEX idx_original_name (original_name),
            INDEX idx_sanitized_name (sanitized_name),
            INDEX idx_table_name (table_name),
            UNIQUE KEY unique_category_field (category, original_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
        """
        cursor.execute(create_category_field_mapping)
        db_logger.info("[CREATE] Tabella category_field_mapping creata")
        
        # Inserisci il mapping per categoria
        mapping_records = 0
        for category, table_definitions in category_table_definitions.items():
            table_name = f"{category}_data"
            field_mapping, column_types = prepare_field_mappings(table_definitions)
            
            for field, def_type in table_definitions.items():
                cursor.execute("""
                    INSERT INTO category_field_mapping (category, original_name, sanitized_name, field_type, table_name)
                    VALUES (%s, %s, %s, %s, %s) AS new_mapping
                    ON DUPLICATE KEY UPDATE
                        sanitized_name = new_mapping.sanitized_name,
                        field_type = new_mapping.field_type,
                        table_name = new_mapping.table_name
                """, (category, field, field_mapping[field], column_types[field], table_name))
                mapping_records += 1
        
        db_logger.info(f"[INSERT] Mapping campi per categoria inserito: {mapping_records} record")
        db_logger.info("[COMPLETE] Tabelle metadati ottimizzate create")

def check_and_update_table_structure(cursor, table_name, categories):
    """Verifica e aggiorna la struttura delle tabelle esistenti se necessario."""
    try:
        # Verifica se la tabella esiste
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cursor.fetchone():
            return  # Tabella non esiste, sarà creata normalmente
        
        # Verifica la struttura attuale
        cursor.execute(f"SHOW CREATE TABLE {table_name}")
        create_table_result = cursor.fetchone()
        if not create_table_result:
            return
        
        create_table_sql = create_table_result[1]
        
        # Se la tabella ha la vecchia struttura con PRIMARY KEY (cig, source_file, batch_id)
        if "PRIMARY KEY (`cig`,`source_file`,`batch_id`)" in create_table_sql:
            db_logger.warning(f"[MIGRATE] Tabella {table_name} ha la vecchia struttura, la aggiorno...")
            
            # Crea una tabella temporanea con la nuova struttura
            temp_table_name = f"{table_name}_temp"
            
            # Trova la categoria corrispondente per questa tabella
            category = table_name.replace('_data', '')
            if category in categories:
                db_logger.info(f"[MIGRATE] Creazione tabella temporanea {temp_table_name}")
                
                # Ottieni i campi dalla tabella esistente (esclusi id, cig, created_at, source_file, batch_id)
                cursor.execute(f"DESCRIBE {table_name}")
                existing_columns = cursor.fetchall()
                
                main_fields = []
                for column in existing_columns:
                    column_name = column[0]
                    column_type = column[1]
                    
                    # Salta i campi di sistema
                    if column_name not in ['cig', 'created_at', 'source_file', 'batch_id']:
                        main_fields.append(f"{column_name} {column_type}")
                
                # Crea la tabella temporanea con la nuova struttura
                create_temp_table_sql = f"""
                CREATE TABLE {temp_table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    cig VARCHAR(64) NOT NULL,
                    {', '.join(main_fields)},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_file VARCHAR(255),
                    batch_id VARCHAR(64),
                    INDEX idx_cig (cig),
                    INDEX idx_created_at (created_at),
                    INDEX idx_source_file (source_file),
                    INDEX idx_batch_id (batch_id),
                    INDEX idx_cig_source (cig, source_file),
                    INDEX idx_cig_batch (cig, batch_id),
                    INDEX idx_cig_source_batch (cig, source_file, batch_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
                """
                
                cursor.execute(create_temp_table_sql)
                
                # Copia i dati dalla tabella vecchia alla nuova (rimuovendo duplicati)
                field_list = ', '.join([col[0] for col in existing_columns if col[0] != 'id'])
                
                db_logger.info(f"[MIGRATE] Copia dati da {table_name} a {temp_table_name} (rimuovendo duplicati)")
                copy_data_sql = f"""
                INSERT INTO {temp_table_name} ({field_list})
                SELECT {field_list}
                FROM {table_name}
                GROUP BY cig, source_file, batch_id
                """
                
                cursor.execute(copy_data_sql)
                copied_rows = cursor.rowcount
                
                # Rinomina le tabelle
                db_logger.info(f"[MIGRATE] Sostituisco {table_name} con la nuova struttura")
                cursor.execute(f"DROP TABLE {table_name}")
                cursor.execute(f"RENAME TABLE {temp_table_name} TO {table_name}")
                
                db_logger.info(f"[SUCCESS] Migrazione completata per {table_name}: {copied_rows} righe migrate")
            
    except Exception as e:
        db_logger.error(f"[ERROR] Errore durante aggiornamento struttura {table_name}: {e}")
        # In caso di errore, elimina la tabella temporanea se esiste
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}_temp")
        except:
            pass

def clean_problematic_tables(conn, categories):
    """Pulisce le tabelle problematiche con duplicati e permette un restart pulito."""
    cursor = conn.cursor()
    
    try:
        db_logger.info("[CLEANUP] Inizio pulizia tabelle problematiche...")
        
        for category in categories:
            table_name = f"{category}_data"
            
            try:
                # Verifica se la tabella esiste
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    continue
                
                # Verifica se ci sono duplicati nella tabella
                cursor.execute(f"""
                    SELECT cig, source_file, batch_id, COUNT(*) as count
                    FROM {table_name} 
                    GROUP BY cig, source_file, batch_id 
                    HAVING COUNT(*) > 1 
                    LIMIT 1
                """)
                
                duplicate_found = cursor.fetchone()
                
                if duplicate_found:
                    db_logger.warning(f"[CLEANUP] Trovati duplicati in {table_name}, la ricreo...")
                    
                    # Salva la struttura della tabella
                    cursor.execute(f"SHOW CREATE TABLE {table_name}")
                    create_table_result = cursor.fetchone()
                    if create_table_result:
                        create_table_sql = create_table_result[1]
                        
                        # Se ha la vecchia struttura, la elimina completamente
                        if "PRIMARY KEY (`cig`,`source_file`,`batch_id`)" in create_table_sql:
                            db_logger.info(f"[CLEANUP] Eliminazione completa di {table_name} (struttura obsoleta)")
                            cursor.execute(f"DROP TABLE {table_name}")
                        else:
                            # Se ha la nuova struttura, rimuove solo i duplicati
                            db_logger.info(f"[CLEANUP] Rimozione duplicati da {table_name}")
                            cursor.execute(f"""
                                CREATE TABLE {table_name}_temp AS 
                                SELECT * FROM {table_name} 
                                GROUP BY cig, source_file, batch_id
                            """)
                            cursor.execute(f"DROP TABLE {table_name}")
                            cursor.execute(f"RENAME TABLE {table_name}_temp TO {table_name}")
                else:
                    db_logger.info(f"[OK] Nessun duplicato trovato in {table_name}")
                    
            except Exception as e:
                db_logger.error(f"[ERROR] Errore durante pulizia di {table_name}: {e}")
                continue
        
        # Pulizia anche delle tabelle di tracking per permettere re-import
        db_logger.info("[CLEANUP] Pulizia tabella processed_files per permettere re-import...")
        try:
            cursor.execute("DELETE FROM processed_files WHERE status = 'failed'")
            failed_files_removed = cursor.rowcount
            db_logger.info(f"[CLEANUP] Rimossi {failed_files_removed} file falliti dalla lista processed")
        except Exception as e:
            db_logger.warning(f"[CLEANUP] Errore durante pulizia processed_files: {e}")
        
        conn.commit()
        db_logger.info("[CLEANUP] Pulizia completata con successo")
        
    except Exception as e:
        db_logger.error(f"[ERROR] Errore durante pulizia globale: {e}")
        conn.rollback()
    finally:
        cursor.close()

def check_and_update_table_structure_robust(db_manager, table_name, categories):
    """Versione robusta per verificare e aggiornare la struttura delle tabelle esistenti."""
    try:
        # Verifica se la tabella esiste
        result = db_manager.execute_with_retry(f"SHOW TABLES LIKE '{table_name}'", fetch=True)
        if not result:
            return  # Tabella non esiste, sarà creata normalmente
        
        # Verifica la struttura attuale
        create_table_result = db_manager.execute_with_retry(f"SHOW CREATE TABLE {table_name}", fetch=True)
        if not create_table_result:
            return
        
        create_table_sql = create_table_result[0][1]
        
        # Se la tabella ha la vecchia struttura con PRIMARY KEY (cig, source_file, batch_id)
        if "PRIMARY KEY (`cig`,`source_file`,`batch_id`)" in create_table_sql:
            db_logger.warning(f"[MIGRATE] Tabella {table_name} ha la vecchia struttura, la aggiorno...")
            
            # Crea una tabella temporanea con la nuova struttura
            temp_table_name = f"{table_name}_temp"
            
            # Trova la categoria corrispondente per questa tabella
            category = table_name.replace('_data', '')
            if category in categories:
                db_logger.info(f"[MIGRATE] Creazione tabella temporanea {temp_table_name}")
                
                # Ottieni i campi dalla tabella esistente
                existing_columns = db_manager.execute_with_retry(f"DESCRIBE {table_name}", fetch=True)
                
                main_fields = []
                for column in existing_columns:
                    column_name = column[0]
                    column_type = column[1]
                    
                    # Salta i campi di sistema
                    if column_name not in ['cig', 'created_at', 'source_file', 'batch_id']:
                        main_fields.append(f"{column_name} {column_type}")
                
                # Crea la tabella temporanea con la nuova struttura
                create_temp_table_sql = f"""
                CREATE TABLE {temp_table_name} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    cig VARCHAR(64) NOT NULL,
                    {', '.join(main_fields)},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_file VARCHAR(255),
                    batch_id VARCHAR(64),
                    INDEX idx_cig (cig),
                    INDEX idx_created_at (created_at),
                    INDEX idx_source_file (source_file),
                    INDEX idx_batch_id (batch_id),
                    INDEX idx_cig_source (cig, source_file),
                    INDEX idx_cig_batch (cig, batch_id),
                    INDEX idx_cig_source_batch (cig, source_file, batch_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
                """
                
                db_manager.execute_with_retry(create_temp_table_sql)
                
                # Copia i dati dalla tabella vecchia alla nuova (rimuovendo duplicati)
                field_list = ', '.join([col[0] for col in existing_columns if col[0] != 'id'])
                
                db_logger.info(f"[MIGRATE] Copia dati da {table_name} a {temp_table_name} (rimuovendo duplicati)")
                copy_data_sql = f"""
                INSERT INTO {temp_table_name} ({field_list})
                SELECT {field_list}
                FROM {table_name}
                GROUP BY cig, source_file, batch_id
                """
                
                db_manager.execute_with_retry(copy_data_sql)
                
                # Rinomina le tabelle
                db_logger.info(f"[MIGRATE] Sostituisco {table_name} con la nuova struttura")
                db_manager.execute_with_retry(f"DROP TABLE {table_name}")
                db_manager.execute_with_retry(f"RENAME TABLE {temp_table_name} TO {table_name}")
                
                db_logger.info(f"[SUCCESS] Migrazione completata per {table_name}")
            
    except Exception as e:
        db_logger.error(f"[ERROR] Errore durante aggiornamento struttura {table_name}: {e}")
        # In caso di errore, elimina la tabella temporanea se esiste
        try:
            db_manager.execute_with_retry(f"DROP TABLE IF EXISTS {table_name}_temp")
        except:
            pass

def create_metadata_tables_robust(db_manager, category_table_definitions):
    """Versione robusta per creare tabelle metadati ottimizzate."""
    with LogContext(db_logger, "creazione tabelle metadati ottimizzate robuste"):
        
        # Crea tabella per tracciare i file processati (invariata)
        create_processed_files = """
        CREATE TABLE IF NOT EXISTS processed_files (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_name VARCHAR(255) UNIQUE,
            category VARCHAR(100),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_count INT,
            status ENUM('completed', 'failed') DEFAULT 'completed',
            error_message TEXT,
            INDEX idx_file_name (file_name),
            INDEX idx_category (category),
            INDEX idx_status (status),
            INDEX idx_processed_at (processed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
        """
        db_manager.execute_with_retry(create_processed_files)
        db_logger.info("[CREATE] Tabella processed_files ottimizzata creata")
        
        # Crea tabella per mapping campi per categoria
        create_category_field_mapping = """
        CREATE TABLE IF NOT EXISTS category_field_mapping (
            id INT AUTO_INCREMENT PRIMARY KEY,
            category VARCHAR(100),
            original_name VARCHAR(255),
            sanitized_name VARCHAR(64),
            field_type VARCHAR(50),
            table_name VARCHAR(100),
            INDEX idx_category (category),
            INDEX idx_original_name (original_name),
            INDEX idx_sanitized_name (sanitized_name),
            INDEX idx_table_name (table_name),
            UNIQUE KEY unique_category_field (category, original_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
        """
        db_manager.execute_with_retry(create_category_field_mapping)
        db_logger.info("[CREATE] Tabella category_field_mapping creata")
        
        # Inserisci il mapping per categoria
        mapping_records = 0
        for category, table_definitions in category_table_definitions.items():
            table_name = f"{category}_data"
            field_mapping, column_types = prepare_field_mappings(table_definitions)
            
            for field, def_type in table_definitions.items():
                insert_mapping_sql = """
                    INSERT INTO category_field_mapping (category, original_name, sanitized_name, field_type, table_name)
                    VALUES (%s, %s, %s, %s, %s) AS new_mapping
                    ON DUPLICATE KEY UPDATE
                        sanitized_name = new_mapping.sanitized_name,
                        field_type = new_mapping.field_type,
                        table_name = new_mapping.table_name
                """
                
                db_manager.execute_with_retry(insert_mapping_sql, params=(category, field, field_mapping[field], column_types[field], table_name))
                mapping_records += 1
        
        db_logger.info(f"[INSERT] Mapping campi per categoria inserito: {mapping_records} record")
        db_logger.info("[COMPLETE] Tabelle metadati ottimizzate create")

def analyze_file_before_processing(json_file):
    """Analizza un file prima del processing per rilevare potenziali problemi."""
    try:
        file_path = Path(json_file)
        file_name = file_path.name
        
        # Ottieni informazioni sul file
        file_size_bytes = file_path.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        file_size_gb = file_size_mb / 1024
        
        # Memoria attuale del sistema
        memory_info = psutil.virtual_memory()
        available_memory_gb = memory_info.available / (1024**3)
        used_memory_gb = memory_info.used / (1024**3)
        memory_percent = memory_info.percent
        
        # Stima record nel file (campionamento rapido)
        estimated_records = 0
        sample_lines = 0
        max_sample = 1000  # Campiona max 1000 righe per velocità
        
        with open(json_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    sample_lines += 1
                if sample_lines >= max_sample:
                    break
        
        # Stima totale righe nel file
        if sample_lines > 0:
            # Stima basata su dimensione file e dimensione media delle righe campionate
            with open(json_file, 'r', encoding='utf-8') as f:
                sample_data = f.read(min(file_size_bytes, 1024 * 1024))  # Leggi max 1MB per campione
                if sample_data:
                    lines_in_sample = len([line for line in sample_data.split('\n') if line.strip()])
                    if lines_in_sample > 0:
                        avg_line_size = len(sample_data.encode('utf-8')) / lines_in_sample
                        estimated_records = int(file_size_bytes / avg_line_size)
        
        # Stima memoria necessaria per processing
        avg_record_size_bytes = 2048  # Stima 2KB per record in memoria
        estimated_memory_gb = (estimated_records * avg_record_size_bytes) / (1024**3)
        
        # Calcola ratio rischio
        memory_risk_ratio = estimated_memory_gb / available_memory_gb if available_memory_gb > 0 else float('inf')
        
        # Logging dettagliato
        import_logger.info("="*80)
        import_logger.info(f"📋 [ANALISI PRE-PROCESSING] File: {file_name}")
        import_logger.info(f"📏 [DIMENSIONI] File: {file_size_mb:.1f}MB ({file_size_gb:.2f}GB)")
        import_logger.info(f"📊 [RECORDS] Stimati: {estimated_records:,} record")
        import_logger.info(f"🧠 [MEMORIA] Sistema: {used_memory_gb:.1f}GB/{memory_info.total/(1024**3):.1f}GB ({memory_percent:.1f}%)")
        import_logger.info(f"🧠 [MEMORIA] Disponibile: {available_memory_gb:.1f}GB")
        import_logger.info(f"🧠 [MEMORIA] Stimata necessaria: {estimated_memory_gb:.1f}GB")
        import_logger.info(f"⚠️  [RISCHIO] Memory ratio: {memory_risk_ratio:.2f} ({'ALTO' if memory_risk_ratio > 0.7 else 'MEDIO' if memory_risk_ratio > 0.4 else 'BASSO'})")
        
        # Determina se il file è problematico
        is_problematic = False
        risk_factors = []
        
        if file_size_gb > 2.0:
            risk_factors.append("File molto grande (>2GB)")
            is_problematic = True
            
        if estimated_records > 3_000_000:
            risk_factors.append(f"Troppi record ({estimated_records:,} > 3M)")
            is_problematic = True
            
        if memory_risk_ratio > 0.8:
            risk_factors.append(f"Memoria insufficiente (ratio: {memory_risk_ratio:.2f})")
            is_problematic = True
            
        if available_memory_gb < 2.0:
            risk_factors.append(f"RAM disponibile bassa ({available_memory_gb:.1f}GB < 2GB)")
            is_problematic = True
        
        if is_problematic:
            import_logger.warning(f"🚨 [ALERT] File POTENZIALMENTE PROBLEMATICO!")
            for factor in risk_factors:
                import_logger.warning(f"🚨 [FACTOR] {factor}")
            import_logger.warning(f"🚨 [STRATEGY] Userò processing ridotto per questo file")
        else:
            import_logger.info(f"✅ [OK] File sembra processabile normalmente")
        
        import_logger.info("="*80)
        
        return {
            'file_name': file_name,
            'file_size_mb': file_size_mb,
            'file_size_gb': file_size_gb,
            'estimated_records': estimated_records,
            'estimated_memory_gb': estimated_memory_gb,
            'available_memory_gb': available_memory_gb,
            'memory_risk_ratio': memory_risk_ratio,
            'is_problematic': is_problematic,
            'risk_factors': risk_factors
        }
        
    except Exception as e:
        import_logger.error(f"🚨 [ERROR] Errore nell'analisi pre-processing di {json_file}: {e}")
        return None

def process_file_with_monitoring(json_file, category, current_table_definitions, progress_tracker):
    """Processa un file con monitoring avanzato e gestione errori robusta."""
    file_name = Path(json_file).name
    
    # STEP 1: Analisi pre-processing
    file_analysis = analyze_file_before_processing(json_file)
    if not file_analysis:
        import_logger.error(f"🚨 [SKIP] Impossibile analizzare {file_name}, lo salto")
        return False
    
    # STEP 2: Verifica se già processato
    with DatabaseManager.get_pooled_connection() as conn_check:
        if is_file_processed(conn_check.connection, file_name):
            import_logger.info(f"✅ [OK] File già processato: {file_name}")
            progress_tracker.processed_files += 1
            return True
    
    # STEP 3: Configura parametri basati sull'analisi
    original_batch_size = BATCH_SIZE
    processing_batch_size = BATCH_SIZE
    
    if file_analysis['is_problematic']:
        # Riduci batch size per file problematici
        processing_batch_size = min(25000, BATCH_SIZE // 3)  # Max 25k per file problematici
        import_logger.warning(f"🔧 [ADAPT] Batch size ridotto: {BATCH_SIZE:,} → {processing_batch_size:,}")
        
        # Log memoria aggiuntivo per file problematici
        log_memory_status(import_logger, f"PRE-PROCESSING {file_name}")
    
    # STEP 4: Processing con monitoring
    try:
        import_logger.info(f"🚀 [START] Inizio processing {file_name}")
        start_time = time.time()
        
        # Inizia il tracking del file
        progress_tracker.start_file(file_name, file_analysis['estimated_records'])
        
        batch_id = f"{int(time.time())}_{progress_tracker.processed_files + 1}"
        file_chunks = []
        current_chunk = []
        processed_records_in_file = 0
        
        # Leggi il file e crea i chunk con monitoring
        chunk_creation_start = time.time()
        with open(json_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    current_chunk.append((record, file_name, category))
                    processed_records_in_file += 1
                    
                    # Controllo memoria ogni 10k record per file problematici
                    if file_analysis['is_problematic'] and processed_records_in_file % 10000 == 0:
                        memory_info = psutil.virtual_memory()
                        if memory_info.percent > 85:  # Soglia di sicurezza
                            import_logger.warning(f"⚠️  [MEMORY] RAM alta ({memory_info.percent:.1f}%) al record {processed_records_in_file:,}")
                    
                    # Usa batch size adattivo
                    if len(current_chunk) >= processing_batch_size:
                        file_chunks.append((current_chunk, file_name, batch_id, current_table_definitions, category))
                        current_chunk = []
                        
                except json.JSONDecodeError as e:
                    import_logger.warning(f"⚠️  [JSON] Errore parsing riga {line_num} in {file_name}: {e}")
                    continue
                except Exception as e:
                    import_logger.error(f"🚨 [ERROR] Errore inaspettato riga {line_num} in {file_name}: {e}")
                    continue
        
        if current_chunk:
            file_chunks.append((current_chunk, file_name, batch_id, current_table_definitions, category))
        
        chunk_creation_time = time.time() - chunk_creation_start
        import_logger.info(f"📦 [CHUNKS] Creati {len(file_chunks)} chunk in {chunk_creation_time:.1f}s")
        import_logger.info(f"📊 [RECORDS] Record effettivi: {processed_records_in_file:,} (stimati: {file_analysis['estimated_records']:,})")
        
        # STEP 5: Processa i chunk con timeout monitoring
        records_completed_in_file = 0
        chunk_start_time = time.time()
        
        for chunk_idx, chunk_data in enumerate(file_chunks, 1):
            try:
                chunk_records = len(chunk_data[0])
                
                # Log dettagliato per file problematici
                if file_analysis['is_problematic']:
                    memory_before = psutil.virtual_memory()
                    import_logger.info(f"🔄 [CHUNK] {chunk_idx}/{len(file_chunks)} - RAM prima: {memory_before.percent:.1f}%")
                
                batch_logger.info(f"Processing chunk {chunk_idx}/{len(file_chunks)} ({chunk_records:,} record) per categoria '{category}'")
                
                # Timeout per chunk singolo (per rilevare blocchi)
                chunk_process_start = time.time()
                process_chunk_sequential(chunk_data)
                chunk_process_time = time.time() - chunk_process_start
                
                # Verifica se il chunk ha impiegato troppo tempo
                if chunk_process_time > 300:  # 5 minuti per chunk
                    import_logger.warning(f"⏰ [SLOW] Chunk {chunk_idx} ha impiegato {chunk_process_time:.1f}s (molto lento)")
                
                records_completed_in_file += chunk_records
                progress_tracker.update_file_progress(records_completed_in_file)
                
                # Log memoria dopo chunk problematici
                if file_analysis['is_problematic']:
                    memory_after = psutil.virtual_memory()
                    import_logger.info(f"✅ [CHUNK] {chunk_idx}/{len(file_chunks)} completato - RAM dopo: {memory_after.percent:.1f}%")
                
            except Exception as e:
                import_logger.error(f"🚨 [ERROR] Errore nel chunk {chunk_idx} di {file_name}: {e}")
                log_error_with_context(batch_logger, e, f"chunk {chunk_idx}", file_name)
                continue
        
        # STEP 6: Finalizzazione
        total_time = time.time() - start_time
        
        with DatabaseManager.get_pooled_connection() as conn_mark:
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file)
        
        progress_tracker.finish_file(processed_records_in_file)
        
        import_logger.info(f"✅ [SUCCESS] {file_name} completato in {total_time:.1f}s")
        import_logger.info(f"📊 [FINAL] Record processati: {records_completed_in_file:,}/{processed_records_in_file:,}")
        
        # Log finale memoria per file problematici
        if file_analysis['is_problematic']:
            log_memory_status(import_logger, f"POST-PROCESSING {file_name}")
        
        # Cleanup memoria
        del file_chunks
        del current_chunk
        gc.collect()
        
        return True
        
    except Exception as e:
        total_time = time.time() - start_time
        error_message = str(e)
        
        import_logger.error(f"🚨 [FATAL] Errore critico in {file_name} dopo {total_time:.1f}s: {error_message}")
        log_error_with_context(import_logger, e, "processing file", file_name)
        
        # Marca come fallito con dettagli
        with DatabaseManager.get_pooled_connection() as conn_mark:
            detailed_error = f"CRASH dopo {total_time:.1f}s - {error_message} - Memory: {psutil.virtual_memory().percent:.1f}%"
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file, 'failed', detailed_error)
        
        # Cleanup memoria di emergenza
        gc.collect()
        
        return False

def process_file_streaming(json_file, category, current_table_definitions, progress_tracker):
    """
    Processa un file con approccio STREAMING per evitare Out of Memory.
    Legge e processa riga per riga senza mai caricare tutto il file in memoria.
    """
    file_name = Path(json_file).name
    
    # STEP 1: Analisi pre-processing (rimane uguale)
    file_analysis = analyze_file_before_processing(json_file)
    if not file_analysis:
        import_logger.error(f"🚨 [SKIP] Impossibile analizzare {file_name}, lo salto")
        return False
    
    # STEP 2: Verifica se già processato
    with DatabaseManager.get_pooled_connection() as conn_check:
        if is_file_processed(conn_check.connection, file_name):
            import_logger.info(f"✅ [OK] File già processato: {file_name}")
            progress_tracker.processed_files += 1
            return True
    
    # STEP 3: Configura parametri per STREAMING
    # Per file problematici usa chunk ancora più piccoli
    if file_analysis['is_problematic']:
        streaming_batch_size = 5000  # Solo 5k record per volta per file grandi
        import_logger.warning(f"🔧 [STREAMING] Batch ultra-ridotto: {streaming_batch_size:,} per file problematico")
    else:
        streaming_batch_size = 15000  # 15k per file normali
    
    try:
        import_logger.info(f"🌊 [STREAMING] Inizio processing streaming di {file_name}")
        start_time = time.time()
        
        # Inizia il tracking del file
        progress_tracker.start_file(file_name, file_analysis['estimated_records'])
        
        batch_id = f"{int(time.time())}_{progress_tracker.processed_files + 1}"
        current_chunk = []
        processed_records_in_file = 0
        total_chunks_processed = 0
        
        # STREAMING PROCESSING: leggi riga per riga
        with open(json_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    current_chunk.append((record, file_name, category))
                    processed_records_in_file += 1
                    
                    # PROCESSA IL CHUNK IMMEDIATAMENTE quando è pieno
                    if len(current_chunk) >= streaming_batch_size:
                        success = process_chunk_immediately(
                            current_chunk, 
                            file_name, 
                            batch_id, 
                            current_table_definitions, 
                            category,
                            total_chunks_processed + 1,
                            file_analysis['is_problematic']
                        )
                        
                        if success:
                            total_chunks_processed += 1
                            progress_tracker.update_file_progress(processed_records_in_file)
                        
                        # LIBERA IMMEDIATAMENTE LA MEMORIA
                        current_chunk = []
                        gc.collect()  # Forza garbage collection
                        
                        # Log memoria ogni 10 chunk per file problematici
                        if file_analysis['is_problematic'] and total_chunks_processed % 10 == 0:
                            memory_info = psutil.virtual_memory()
                            import_logger.info(f"🌊 [STREAMING] Chunk {total_chunks_processed} - RAM: {memory_info.percent:.1f}%")
                            
                            # EMERGENCY BRAKE: se la RAM supera il 90%, forza pausa
                            if memory_info.percent > 90:
                                import_logger.warning(f"🚨 [EMERGENCY] RAM critica ({memory_info.percent:.1f}%), pausa 2s...")
                                time.sleep(2)
                                gc.collect()
                    
                    # Controllo memoria ogni 1000 record per rilevare problemi
                    if processed_records_in_file % 1000 == 0:
                        memory_info = psutil.virtual_memory()
                        if memory_info.percent > 95:
                            import_logger.error(f"🚨 [ABORT] RAM critica ({memory_info.percent:.1f}%) - interrompo file")
                            raise MemoryError(f"RAM critica: {memory_info.percent:.1f}%")
                        
                except json.JSONDecodeError as e:
                    import_logger.warning(f"⚠️  [JSON] Errore parsing riga {line_num} in {file_name}: {e}")
                    continue
                except Exception as e:
                    import_logger.error(f"🚨 [ERROR] Errore inaspettato riga {line_num} in {file_name}: {e}")
                    continue
        
        # Processa l'ultimo chunk parziale
        if current_chunk:
            success = process_chunk_immediately(
                current_chunk, 
                file_name, 
                batch_id, 
                current_table_definitions, 
                category,
                total_chunks_processed + 1,
                file_analysis['is_problematic']
            )
            if success:
                total_chunks_processed += 1
            current_chunk = []
        
        # STEP 4: Finalizzazione
        total_time = time.time() - start_time
        
        with DatabaseManager.get_pooled_connection() as conn_mark:
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file)
        
        progress_tracker.finish_file(processed_records_in_file)
        
        import_logger.info(f"✅ [SUCCESS] {file_name} completato con STREAMING in {total_time:.1f}s")
        import_logger.info(f"📊 [FINAL] Record: {processed_records_in_file:,}, Chunk: {total_chunks_processed}")
        
        # Cleanup finale
        gc.collect()
        final_memory = psutil.virtual_memory()
        import_logger.info(f"🧠 [FINAL] RAM finale: {final_memory.percent:.1f}%")
        
        return True
        
    except MemoryError as e:
        import_logger.error(f"🚨 [OOM] Out of Memory in {file_name}: {e}")
        
        # Marca come fallito con dettagli OOM
        with DatabaseManager.get_pooled_connection() as conn_mark:
            detailed_error = f"OUT OF MEMORY - {e} - Processed: {processed_records_in_file:,} records"
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file, 'failed', detailed_error)
        
        # Cleanup di emergenza
        current_chunk = []
        gc.collect()
        
        return False
        
    except Exception as e:
        total_time = time.time() - start_time
        error_message = str(e)
        
        import_logger.error(f"🚨 [FATAL] Errore critico in {file_name} dopo {total_time:.1f}s: {error_message}")
        
        # Marca come fallito
        with DatabaseManager.get_pooled_connection() as conn_mark:
            detailed_error = f"CRASH dopo {total_time:.1f}s - {error_message} - Records: {processed_records_in_file:,}"
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file, 'failed', detailed_error)
        
        # Cleanup di emergenza
        current_chunk = []
        gc.collect()
        
        return False

def process_chunk_immediately(chunk_data, file_name, batch_id, table_definitions, category, chunk_num, is_problematic_file):
    """
    Processa immediatamente un chunk di dati con gestione memoria e logging migliorati.
    
    Args:
        chunk_data: Lista di record JSON (non tuple)
        file_name: Nome del file
        batch_id: ID del batch
        table_definitions: Definizioni tabelle
        category: Categoria
        chunk_num: Numero chunk
        is_problematic_file: Se il file è considerato problematico
    """
    import_logger.info(f" [STREAMING] Chunk {chunk_num} ({len(chunk_data):,} record) - categoria '{category}'{' - FILE PROBLEMATICO' if is_problematic_file else ''}")
    
    start_time = time.time()
    success = False
    
    try:
        # Converti il formato per process_batch se necessario
        if chunk_data and not isinstance(chunk_data[0], tuple):
            # Converti da lista di record a lista di tuple (record, file_name, category)
            formatted_chunk = [(record, file_name, category) for record in chunk_data]
        else:
            formatted_chunk = chunk_data
        
        # Usa una connessione dal pool - il context manager restituisce un oggetto con .connection
        with DatabaseManager.get_pooled_connection() as db_context:
            success = process_batch(
                db_context.connection, 
                formatted_chunk, 
                table_definitions, 
                batch_id, 
                category=category
            )
        
        elapsed_time = time.time() - start_time
        
        if success:
            import_logger.info(f"[COMPLETE] Completato processing batch in {elapsed_time:.1f}s")
        else:
            import_logger.error(f"[FAILED] Batch fallito dopo {elapsed_time:.1f}s")
            
        return success
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        import_logger.error(f"🚨 [CHUNK-ERROR] Errore processing chunk {chunk_num}: {e} (dopo {elapsed_time:.1f}s)")
        return False

def choose_optimal_processing_strategy(file_analysis):
    """
    Sceglie la strategia di processing ottimale basata sull'analisi del file.
    
    Ora preferisce il DYNAMIC STREAMING come strategia principale.
    """
    file_size_gb = file_analysis.get('file_size_gb', 0)
    estimated_records = file_analysis.get('estimated_records', 0)
    memory_risk_ratio = file_analysis.get('memory_risk_ratio', 0)
    total_ram_gb = file_analysis.get('system_ram_gb', 8)
    
    # NUOVA STRATEGIA PRINCIPALE: DYNAMIC STREAMING
    # Usa il dynamic streaming per tutti i file, permette adattamento automatico
    if file_size_gb <= 4 and estimated_records <= 5000000:  # File ragionevolmente grandi
        return {
            'strategy': 'dynamic_streaming',
            'batch_size': 10000,  # Batch iniziale, verrà adattato dinamicamente
            'description': 'Streaming dinamico con adattamento automatico RAM',
            'rationale': f'File {file_size_gb:.2f}GB - Streaming dinamico ottimale'
        }
    
    # FALLBACK per file enormi (>4GB) - usa streaming sicuro fisso
    elif file_size_gb > 4 or estimated_records > 5000000:
        return {
            'strategy': 'streaming_safe',
            'batch_size': 3000,
            'description': 'Streaming sicuro per file molto grandi',
            'rationale': f'File {file_size_gb:.2f}GB ({estimated_records:,} record) - troppo grande per dynamic'
        }
    
    # FALLBACK generico - dynamic streaming con batch conservativo
    else:
        return {
            'strategy': 'dynamic_streaming',
            'batch_size': 5000,  # Batch iniziale conservativo
            'description': 'Streaming dinamico conservativo',
            'rationale': f'Strategia dinamica conservativa per file sconosciuto'
        }

def process_file_memory_optimized(json_file, category, current_table_definitions, progress_tracker, strategy):
    """
    Processing ottimizzato per velocità: carica tutto in memoria.
    Usa solo per file che sicuramente stanno in memoria.
    """
    file_name = Path(json_file).name
    
    # Verifica se già processato
    with DatabaseManager.get_pooled_connection() as conn_check:
        if is_file_processed(conn_check.connection, file_name):
            import_logger.info(f"✅ [OK] File già processato: {file_name}")
            progress_tracker.processed_files += 1
            return True
    
    try:
        import_logger.info(f"🚀 [MEMORY-OPT] Processing ad alta velocità: {file_name}")
        import_logger.info(f"🚀 [STRATEGY] {strategy['description']} (batch: {strategy['batch_size']:,})")
        
        start_time = time.time()
        
        # Carica tutto il file in memoria (è sicuro secondo l'analisi)
        all_records = []
        processed_records_in_file = 0
        
        with open(json_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    all_records.append((record, file_name, category))
                    processed_records_in_file += 1
                    
                except json.JSONDecodeError as e:
                    import_logger.warning(f"⚠️  [JSON] Errore parsing riga {line_num}: {e}")
                    continue
        
        import_logger.info(f"📥 [LOADED] {processed_records_in_file:,} record caricati in memoria")
        
        # Inizia tracking
        progress_tracker.start_file(file_name, processed_records_in_file)
        
        # Processa in batch grandi per massima velocità
        batch_id = f"{int(time.time())}_{progress_tracker.processed_files + 1}"
        batch_size = strategy['batch_size']
        total_batches = math.ceil(len(all_records) / batch_size)
        
        import_logger.info(f"🔄 [BATCHING] {total_batches} batch da {batch_size:,} record")
        
        records_completed = 0
        for i in range(0, len(all_records), batch_size):
            batch_num = (i // batch_size) + 1
            batch_chunk = all_records[i:i + batch_size]
            
            batch_logger.info(f"🚀 [FAST-BATCH] {batch_num}/{total_batches} ({len(batch_chunk):,} record)")
            
            # Processing veloce - usa process_chunk_immediately che è corretto
            batch_id_chunk = f"{batch_id}_{batch_num}"
            success = process_chunk_immediately(
                batch_chunk, file_name, batch_id_chunk,
                current_table_definitions, category, batch_num, False
            )
            
            if not success:
                import_logger.error(f"❌ [FAST-BATCH] Batch {batch_num} fallito")
                # Fallback a streaming
                return process_file_streaming(json_file, category, current_table_definitions, progress_tracker)
            
            records_completed += len(batch_chunk)
            progress_tracker.update_file_progress(records_completed)
        
        # Finalizza
        total_time = time.time() - start_time
        speed = processed_records_in_file / total_time if total_time > 0 else 0
        
        with DatabaseManager.get_pooled_connection() as conn_mark:
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file)
        
        progress_tracker.finish_file(processed_records_in_file)
        
        import_logger.info(f"✅ [SPEED-SUCCESS] {file_name} completato in {total_time:.1f}s ({speed:.0f} rec/s)")
        
        # Cleanup
        del all_records
        gc.collect()
        
        return True
        
    except Exception as e:
        import_logger.error(f"🚨 [SPEED-ERROR] Errore in processing veloce: {e}")
        
        # Fallback a streaming in caso di errore
        import_logger.warning(f"🔄 [FALLBACK] Provo con streaming come backup...")
        return process_file_streaming(json_file, category, current_table_definitions, progress_tracker)

def process_file_balanced_batching(json_file, category, current_table_definitions, progress_tracker, strategy):
    """
    Processing bilanciato: batch medi per bilanciare velocità e memoria.
    """
    file_name = Path(json_file).name
    
    # Verifica se già processato
    with DatabaseManager.get_pooled_connection() as conn_check:
        if is_file_processed(conn_check.connection, file_name):
            import_logger.info(f"✅ [OK] File già processato: {file_name}")
            progress_tracker.processed_files += 1
            return True
    
    try:
        import_logger.info(f"⚖️  [BALANCED] Processing bilanciato: {file_name}")
        import_logger.info(f"⚖️  [STRATEGY] {strategy['description']} (batch: {strategy['batch_size']:,})")
        
        start_time = time.time()
        batch_id = f"{int(time.time())}_{progress_tracker.processed_files + 1}"
        batch_size = strategy['batch_size']
        
        current_batch = []
        processed_records_in_file = 0
        total_batches_processed = 0
        
        # Stima record per tracking
        file_analysis = analyze_file_before_processing(json_file)
        estimated_records = file_analysis['estimated_records'] if file_analysis else 0
        progress_tracker.start_file(file_name, estimated_records)
        
        with open(json_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    current_batch.append((record, file_name, category))
                    processed_records_in_file += 1
                    
                    # Processa batch quando è pieno
                    if len(current_batch) >= batch_size:
                        total_batches_processed += 1
                        
                        batch_logger.info(f"⚖️  [BALANCED-BATCH] {total_batches_processed} ({len(current_batch):,} record)")
                        
                        chunk_args = (current_batch, file_name, batch_id, current_table_definitions, category)
                        process_chunk_sequential(chunk_args)
                        
                        progress_tracker.update_file_progress(processed_records_in_file)
                        
                        # Libera memoria del batch
                        current_batch = []
                        
                        # Controllo memoria ogni 10 batch
                        if total_batches_processed % 10 == 0:
                            memory_info = psutil.virtual_memory()
                            import_logger.info(f"⚖️  [BALANCED] Batch {total_batches_processed} - RAM: {memory_info.percent:.1f}%")
                            
                            if memory_info.percent > 85:
                                gc.collect()  # Garbage collection preventiva
                    
                except json.JSONDecodeError as e:
                    import_logger.warning(f"⚠️  [JSON] Errore parsing riga {line_num}: {e}")
                    continue
        
        # Processa ultimo batch parziale
        if current_batch:
            total_batches_processed += 1
            batch_logger.info(f"⚖️  [BALANCED-FINAL] Ultimo batch ({len(current_batch):,} record)")
            
            chunk_args = (current_batch, file_name, batch_id, current_table_definitions, category)
            process_chunk_sequential(chunk_args)
        
        # Finalizza
        total_time = time.time() - start_time
        speed = processed_records_in_file / total_time if total_time > 0 else 0
        
        with DatabaseManager.get_pooled_connection() as conn_mark:
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file)
        
        progress_tracker.finish_file(processed_records_in_file)
        
        import_logger.info(f"✅ [BALANCED-SUCCESS] {file_name} completato in {total_time:.1f}s ({speed:.0f} rec/s)")
        
        # Cleanup
        gc.collect()
        
        return True
        
    except Exception as e:
        import_logger.error(f"🚨 [BALANCED-ERROR] Errore in processing bilanciato: {e}")
        
        # Fallback a streaming
        import_logger.warning(f"🔄 [FALLBACK] Provo con streaming...")
        return process_file_streaming(json_file, category, current_table_definitions, progress_tracker)

def process_file_streaming_with_custom_batch(json_file, category, current_table_definitions, progress_tracker, strategy):
    """
    Process file con streaming e batch size DINAMICO che si adatta alla RAM disponibile.
    """
    start_time = time.time()
    file_name = os.path.basename(json_file)
    processed_records_in_file = 0
    chunk_num = 0
    
    # Impostazioni iniziali
    initial_batch_size = strategy.get('batch_size', 5000)
    dynamic_recalc_interval = 10  # Ricalcola ogni 10 chunk
    
    import_logger.info(f"🚀 [DYNAMIC-STREAMING] Avvio processing dinamico: {file_name}")
    import_logger.info(f"    📊 Strategia base: {strategy['strategy']} (batch iniziale: {initial_batch_size:,})")
    import_logger.info(f"    🔄 Ricalcolo dinamico ogni {dynamic_recalc_interval} chunk")
    
    try:
        # Verifica se già processato
        with DatabaseManager.get_pooled_connection() as conn_check:
            if is_file_processed(conn_check.connection, file_name):
                import_logger.info(f"✅ [ALREADY-DONE] File già processato: {file_name}")
                return True
        
        # Calcola batch dinamico iniziale
        current_batch_size, ram_info = calculate_dynamic_batch_size()
        log_dynamic_batch_decision(import_logger, current_batch_size, ram_info)
        
        # Apri il file in streaming
        with open(json_file, 'r', encoding='utf-8') as f:
            # Leggi riga per riga e accumula i record
            current_chunk = []
            
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    current_chunk.append(record)
                    
                    # Quando raggiungiamo la dimensione del chunk corrente
                    if len(current_chunk) >= current_batch_size:
                        chunk_num += 1
                        
                        # Log del chunk
                        is_problematic = len(current_chunk) <= 10000  # Considera problematico se usa batch piccoli
                        import_logger.info(f" [STREAMING] Chunk {chunk_num} ({len(current_chunk):,} record) - categoria '{category}'{' - BATCH DINAMICO' if not is_problematic else ' - BATCH SICURO'}")
                        
                        # Processa il chunk
                        batch_id = f"{int(time.time())}_{chunk_num}"
                        success = process_chunk_immediately(
                            current_chunk, file_name, batch_id, 
                            current_table_definitions, category, chunk_num, is_problematic
                        )
                        
                        if success:
                            processed_records_in_file += len(current_chunk)
                            progress_tracker.update_file_progress(len(current_chunk))
                            
                            # Log progresso RAM
                            ram = psutil.virtual_memory()
                            import_logger.info(f"✅ [STREAMING] Chunk {chunk_num} completato - RAM: {ram.percent:.1f}%")
                            
                            # Aggiorna progress ogni chunk
                            if chunk_num % 5 == 0:  # Ogni 5 chunk
                                speed = processed_records_in_file / (time.time() - start_time)
                                remaining_time = (2400000 - processed_records_in_file) / speed if speed > 0 else 0
                                hours, remainder = divmod(remaining_time, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                
                                import_logger.info(f"[BATCH] Batch: {processed_records_in_file:,}/2,397,441 ({processed_records_in_file/2397441*100:.1f}%) | {speed:.0f} rec/s | RAM: ETA: {int(hours)}:{int(minutes):02d}:{int(seconds):02d}")
                            
                            # RICALCOLO DINAMICO ogni N chunk
                            if chunk_num % dynamic_recalc_interval == 0:
                                old_batch_size = current_batch_size
                                current_batch_size, ram_info = calculate_dynamic_batch_size()
                                
                                if current_batch_size != old_batch_size:
                                    import_logger.info(f"🔄 [DYNAMIC] Aggiornamento batch size: {old_batch_size:,} → {current_batch_size:,}")
                                    log_dynamic_batch_decision(import_logger, current_batch_size, ram_info)
                                else:
                                    ram_percent = ram_info.get('used_percent', 0)
                                    batch_category = ram_info.get('batch_category', 'unknown')
                                    import_logger.info(f"✓ [DYNAMIC] Batch confermato: {current_batch_size:,} ({batch_category}) - RAM: {ram_percent:.1f}%")
                        else:
                            import_logger.error(f"❌ [STREAMING] Chunk {chunk_num} fallito, interrompo processing")
                            # Marca come fallito immediatamente
                            with DatabaseManager.get_pooled_connection() as conn_mark:
                                detailed_error = f"DYNAMIC-STREAMING FAIL - Chunk {chunk_num} failed"
                                mark_file_processed(conn_mark.connection, file_name, processed_records_in_file, 'failed', detailed_error)
                            return False
                        
                        # Reset chunk
                        current_chunk = []
                        
                        # Cleanup memoria
                        gc.collect()
                        
                        # Controlla memoria critica (emergenza)
                        ram = psutil.virtual_memory()
                        if ram.percent > 90:
                            import_logger.warning(f"⚠️ [EMERGENCY] RAM critica {ram.percent:.1f}% - forzo batch minimo")
                            current_batch_size = 1000
                        elif ram.percent > 95:
                            import_logger.error(f"🚨 [ABORT] RAM troppo alta {ram.percent:.1f}% - interrompo")
                            # Marca come fallito
                            with DatabaseManager.get_pooled_connection() as conn_mark:
                                detailed_error = f"DYNAMIC-STREAMING FAIL - RAM too high: {ram.percent:.1f}%"
                                mark_file_processed(conn_mark.connection, file_name, processed_records_in_file, 'failed', detailed_error)
                            return False
                
                except json.JSONDecodeError as e:
                    import_logger.warning(f"⚠️ [JSON] Riga {line_num} malformata in {file_name}: {e}")
                    continue
            
            # Processa ultimo chunk se presente
            if current_chunk:
                chunk_num += 1
                import_logger.info(f" [STREAMING] Chunk finale {chunk_num} ({len(current_chunk):,} record) - categoria '{category}'")
                
                batch_id = f"{int(time.time())}_{chunk_num}"
                success = process_chunk_immediately(
                    current_chunk, file_name, batch_id, 
                    current_table_definitions, category, chunk_num, True
                )
                
                if success:
                    processed_records_in_file += len(current_chunk)
                    progress_tracker.update_file_progress(len(current_chunk))
                else:
                    import_logger.error(f"❌ [STREAMING] Chunk finale {chunk_num} fallito")
                    # Marca come fallito
                    with DatabaseManager.get_pooled_connection() as conn_mark:
                        detailed_error = f"DYNAMIC-STREAMING FAIL - Final chunk failed"
                        mark_file_processed(conn_mark.connection, file_name, processed_records_in_file, 'failed', detailed_error)
                    return False
        
        # Statistiche finali e validazione
        total_time = time.time() - start_time
        avg_speed = processed_records_in_file / total_time if total_time > 0 else 0
        
        # CONTROLLO CRITICO: Se non abbiamo processato nessun record, è un fallimento
        if processed_records_in_file == 0:
            import_logger.error(f"🚨 [FAIL] File {file_name} completato ma con 0 record processati!")
            with DatabaseManager.get_pooled_connection() as conn_mark:
                detailed_error = f"DYNAMIC-STREAMING FAIL - Zero records processed"
                mark_file_processed(conn_mark.connection, file_name, 0, 'failed', detailed_error)
            return False
        
        import_logger.info(f"🎉 [DYNAMIC-COMPLETE] File {file_name} completato!")
        import_logger.info(f"    📊 Record processati: {processed_records_in_file:,}")
        import_logger.info(f"    ⏱️ Tempo totale: {total_time:.1f}s")
        import_logger.info(f"    ⚡ Velocità media: {avg_speed:.0f} rec/s")
        import_logger.info(f"    🔢 Chunk totali: {chunk_num}")
        
        # Marca come completato SOLO se abbiamo processato record
        with DatabaseManager.get_pooled_connection() as conn_mark:
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file, 'completed')
        
        # Cleanup finale
        gc.collect()
        
        return True
        
    except Exception as e:
        total_time = time.time() - start_time
        error_message = str(e)
        
        import_logger.error(f"🚨 [DYNAMIC-ERROR] Errore in streaming dinamico per {file_name} dopo {total_time:.1f}s: {error_message}")
        
        # Marca come fallito
        with DatabaseManager.get_pooled_connection() as conn_mark:
            detailed_error = f"DYNAMIC-STREAMING FAIL - {error_message}"
            mark_file_processed(conn_mark.connection, file_name, processed_records_in_file, 'failed', detailed_error)
        
        # Cleanup di emergenza
        gc.collect()
        
        return False

def calculate_dynamic_batch_size(target_ram_buffer_percent=20, estimated_bytes_per_record=2048):
    """
    Calcola dinamicamente la dimensione del batch ottimale basandosi sulla RAM disponibile.
    
    Args:
        target_ram_buffer_percent: Percentuale di RAM da mantenere libera come buffer (default: 20%)
        estimated_bytes_per_record: Stima dei byte per record (default: 2KB)
    
    Returns:
        tuple: (batch_size, ram_info_dict)
    """
    try:
        # Ottieni info RAM corrente
        ram = psutil.virtual_memory()
        total_ram_gb = ram.total / (1024**3)
        used_ram_gb = ram.used / (1024**3)
        available_ram_gb = ram.available / (1024**3)
        used_percent = ram.percent
        
        # Calcola RAM utilizzabile (totale - buffer - già usata)
        buffer_ram_gb = total_ram_gb * (target_ram_buffer_percent / 100)
        usable_ram_gb = total_ram_gb - buffer_ram_gb - used_ram_gb
        
        # Assicurati che ci sia RAM utilizzabile
        if usable_ram_gb <= 0:
            return 1000, {  # Batch size minimo di sicurezza
                'status': 'critical_low_ram',
                'total_gb': total_ram_gb,
                'used_gb': used_ram_gb,
                'used_percent': used_percent,
                'available_gb': available_ram_gb,
                'usable_gb': usable_ram_gb,
                'buffer_gb': buffer_ram_gb,
                'reason': 'RAM insufficiente dopo buffer'
            }
        
        # Calcola quanti record possiamo processare con la RAM utilizzabile
        usable_bytes = usable_ram_gb * (1024**3)
        max_records = int(usable_bytes / estimated_bytes_per_record)
        
        # Applica limiti di sicurezza
        min_batch = 1000    # Minimo assoluto
        max_batch = 100000  # Massimo assoluto per evitare problemi di memoria
        
        # Calcola batch size ottimale
        optimal_batch = max(min_batch, min(max_records, max_batch))
        
        # Logica adattiva basata sulla percentuale di RAM utilizzata
        if used_percent < 30:  # RAM molto disponibile
            optimal_batch = min(optimal_batch, 75000)
        elif used_percent < 50:  # RAM moderatamente disponibile
            optimal_batch = min(optimal_batch, 50000)
        elif used_percent < 70:  # RAM limitata
            optimal_batch = min(optimal_batch, 25000)
        elif used_percent < 85:  # RAM critica
            optimal_batch = min(optimal_batch, 10000)
        else:  # RAM molto critica
            optimal_batch = min(optimal_batch, 5000)
        
        return optimal_batch, {
            'status': 'optimal',
            'total_gb': total_ram_gb,
            'used_gb': used_ram_gb,
            'used_percent': used_percent,
            'available_gb': available_ram_gb,
            'usable_gb': usable_ram_gb,
            'buffer_gb': buffer_ram_gb,
            'max_records': max_records,
            'optimal_batch': optimal_batch,
            'batch_category': 'high_performance' if optimal_batch >= 50000 else 'balanced' if optimal_batch >= 20000 else 'conservative' if optimal_batch >= 10000 else 'safe'
        }
        
    except Exception as e:
        import_logger.warning(f"⚠️ [DYNAMIC] Errore calcolo batch dinamico: {e}, uso fallback 5000")
        return 5000, {
            'status': 'error',
            'error': str(e),
            'fallback_batch': 5000
        }

def log_dynamic_batch_decision(logger_instance, batch_size, ram_info):
    """Log dettagliato della decisione del batch dinamico"""
    status = ram_info.get('status', 'unknown')
    
    if status == 'optimal':
        used_percent = ram_info.get('used_percent', 0)
        usable_gb = ram_info.get('usable_gb', 0)
        batch_category = ram_info.get('batch_category', 'unknown')
        max_records = ram_info.get('max_records', 0)
        
        logger_instance.info(f"🔄 [DYNAMIC] Batch dinamico calcolato:")
        logger_instance.info(f"    💾 RAM: {used_percent:.1f}% utilizzata, {usable_gb:.2f}GB utilizzabili")
        logger_instance.info(f"    📊 Capacità massima: {max_records:,} record")
        logger_instance.info(f"    ⚡ Batch ottimale: {batch_size:,} record ({batch_category})")
        logger_instance.info(f"    🎯 Strategia: {'Performance' if batch_size >= 50000 else 'Bilanciata' if batch_size >= 20000 else 'Conservativa' if batch_size >= 10000 else 'Sicura'}")
    
    elif status == 'critical_low_ram':
        used_percent = ram_info.get('used_percent', 0)
        available_gb = ram_info.get('available_gb', 0)
        
        logger_instance.warning(f"⚠️ [DYNAMIC] RAM critica - batch minimo:")
        logger_instance.warning(f"    💾 RAM: {used_percent:.1f}% utilizzata, solo {available_gb:.2f}GB disponibili")
        logger_instance.warning(f"    🛡️ Batch sicurezza: {batch_size:,} record")
    
    elif status == 'error':
        error = ram_info.get('error', 'unknown')
        fallback = ram_info.get('fallback_batch', 5000)
        
        logger_instance.error(f"🚨 [DYNAMIC] Errore calcolo - fallback:")
        logger_instance.error(f"    ❌ Errore: {error}")
        logger_instance.error(f"    🛡️ Batch fallback: {fallback:,} record")

if __name__ == "__main__":
    main()
