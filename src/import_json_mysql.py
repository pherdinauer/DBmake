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
        log_resource_optimization, check_disk_space
    )
except ImportError:
    # Fallback su import relative (quando eseguito come modulo)
    from .database import DatabaseManager
    from .utils import (
        LogContext, log_memory_status, log_performance_stats, 
        log_file_progress, log_batch_progress, log_error_with_context,
        log_resource_optimization, check_disk_space
    )
    from .temp_additions import RecordProcessor, process_batch_parallel

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
    INITIAL_CHUNK_SIZE = calculate_initial_chunk_size()
    MAX_CHUNK_SIZE = calculate_max_chunk_size()
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
    """Helper per logging errori con contesto."""
    context_str = f"[{context}] " if context else ""
    operation_str = f" durante {operation}" if operation else ""
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
            else:
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
                # Controlla se è un codice fiscale
                if re.match(r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$', problematic_value):
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
            
            # Errore 1265: Data truncated
            elif e.errno == 1265:
                adapted = handle_type_compatibility_error(cursor, str(e), table_name)
            
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
                    raise
                else:
                    # Aspetta un po' prima di riprovare
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

def insert_batch_direct(cursor, main_data, table_name, fields):
    """Inserisce i dati direttamente in MySQL senza passare per CSV con adattamento dinamico."""
    if not main_data:
        return 0
        
    # Calcola batch size dinamico iniziale basato su utilizzo RAM aggressivo
    initial_batch_size = calculate_dynamic_insert_batch_size()
    
    # Inizializza l'adaptive batch sizer
    batch_sizer = AdaptiveBatchSizer(initial_batch_size, target_ram_usage=0.8)
    
    # Statistiche RAM dettagliate
    memory_info = psutil.virtual_memory()
    total_ram_gb = memory_info.total / (1024**3)
    used_ram_gb = memory_info.used / (1024**3)
    available_ram_gb = memory_info.available / (1024**3)
    usage_pct = memory_info.percent
    target_usage_pct = 80.0
    
    logger.info(f"[INSERT] Inserimento INTELLIGENTE di {len(main_data):,} record in {table_name}")
    logger.info(f"[INSERT] Batch size aggressivo iniziale: {initial_batch_size:,}")
    logger.info(f"[RAM] RAM: {used_ram_gb:.1f}GB/{total_ram_gb:.1f}GB utilizzata ({usage_pct:.1f}%) | "
               f"Target: {target_usage_pct:.0f}% | Disponibile: {available_ram_gb:.1f}GB")
    
    rows_inserted = 0
    
    # Prepara la query INSERT
    placeholders = ', '.join(['%s'] * len(fields))
    insert_query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
    
    insert_start_time = time.time()
    adaptation_count = 0  # Contatore di adattamenti automatici
    
    try:
        # Processa i dati in batch con dimensioni adattive
        i = 0
        while i < len(main_data):
            # Ottieni il batch size corrente (può essere cambiato dall'adaptive sizer)
            current_batch_size = batch_sizer.current_batch_size
            
            batch_start_time = time.time()
            batch = main_data[i:i + current_batch_size]
            
            # Usa il sistema di inserimento adattivo intelligente
            success = adaptive_insert_with_retry(cursor, insert_query, batch, table_name)
            
            if success:
                rows_inserted += len(batch)
                
                batch_time = time.time() - batch_start_time
                batch_speed = len(batch) / batch_time if batch_time > 0 else 0
                current_ram_usage = psutil.virtual_memory().percent / 100.0
                
                # Auto-tuning del batch size basato su performance
                new_batch_size = batch_sizer.adjust_batch_size(current_ram_usage, batch_speed)
                
                # Log progresso ogni 10k righe con statistiche performance e RAM
                if rows_inserted % 10000 == 0:
                    elapsed_total = time.time() - insert_start_time
                    avg_speed = rows_inserted / elapsed_total if elapsed_total > 0 else 0
                    current_ram = psutil.virtual_memory().used / (1024**3)
                    current_usage = psutil.virtual_memory().percent
                    
                    logger.info(f"[PROGRESS] INSERT: {rows_inserted:,}/{len(main_data):,} righe | "
                              f"Batch: {batch_speed:.0f} rec/s | Media: {avg_speed:.0f} rec/s | "
                              f"RAM: {current_ram:.1f}GB ({current_usage:.1f}%) | Batch size: {current_batch_size:,}")
            else:
                logger.error(f"[FAIL] Inserimento batch fallito dopo adattamenti automatici")
                break
            
            i += current_batch_size
        
        total_time = time.time() - insert_start_time
        avg_speed = rows_inserted / total_time if total_time > 0 else 0
        final_ram_gb = psutil.virtual_memory().used / (1024**3)
        final_usage_pct = psutil.virtual_memory().percent
        final_batch_size = batch_sizer.current_batch_size
        
        logger.info(f"[COMPLETE] INSERT intelligente completato: {rows_inserted:,} righe totali")
        logger.info(f"[PERF] Performance: {avg_speed:.0f} rec/s in {total_time:.1f}s | "
                   f"Batch size finale: {final_batch_size:,} | RAM finale: {final_ram_gb:.1f}GB ({final_usage_pct:.1f}%)")
        
        if adaptation_count > 0:
            logger.info(f"[ADAPT] Sistema adattivo: {adaptation_count} modifiche automatiche alla struttura")
        
        return rows_inserted
        
    except Exception as e:
        logger.error(f"[ERROR] Errore in INSERT intelligente: {e}")
        raise

def process_batch(cursor, batch, table_definitions, batch_id, progress_tracker=None, category=None):
    if not batch:
        return

    file_name = batch[0][1]
    
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
                rows_affected = insert_batch_direct(cursor, main_data, table_name, fields)
            
            # Gestisci i dati JSON separatamente (manteniamo l'approccio precedente per i JSON)
            if json_data:
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
            
            elapsed_time = time.time() - start_time
            log_performance_stats(batch_logger, f"Batch completato per categoria '{category}'", len(batch), elapsed_time)
        
        except mysql.connector.Error as e:
            if e.errno == 1153:  # Packet too large
                batch_logger.warning("Batch troppo grande, riduco la dimensione...")
                raise ValueError("BATCH_TOO_LARGE")
            raise
        except Exception as e:
            log_error_with_context(batch_logger, e, f"batch {batch_id}", "processing")
            raise

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
            with DatabaseManager.get_pooled_connection() as conn:
                cursor = conn.cursor()
                try:
                    process_batch(cursor, chunk, table_definitions, batch_id, category=category)
                    conn.commit()
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
                finally:
                    cursor.close()
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
        with DatabaseManager.get_connection() as tmp_conn:
            create_dynamic_tables_by_category(tmp_conn, category_table_definitions)
        
        # Inizializza il pool di connessioni centralizzato
        DatabaseManager.initialize_pool(pool_size=2)  # Solo 2 connessioni per mono-processo
        
        log_resource_optimization(import_logger)
        
        # Processa categoria per categoria con schema specifico
        total_processed_files = 0
        total_processed_records = 0
        
        for category, category_files in categories.items():
            import_logger.info(f"[CATEGORIA] Processando categoria '{category}' con {len(category_files)} file")
            
            # Usa le definizioni specifiche per questa categoria
            current_table_definitions = category_table_definitions[category]
            
            # Processa i file di questa categoria
            for idx, json_file in enumerate(category_files, 1):
                file_name = Path(json_file).name
                
                # Salta i file già processati con successo
                with DatabaseManager.get_pooled_connection() as conn_check:
                    if is_file_processed(conn_check, file_name):
                        import_logger.info(f"[OK] File già processato: {file_name}")
                        progress_tracker.processed_files += 1  # Aggiorna counter per file già processati
                        continue
                
                # Conta i record nel file per il progresso
                file_record_count = 0
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                file_record_count += 1
                except Exception as e:
                    log_error_with_context(import_logger, e, "conteggio record", file_name)
                    continue
                
                # Inizia il tracking del file
                progress_tracker.start_file(file_name, file_record_count)
                
                batch_id = f"{int(time.time())}_{idx}"
                chunk_size = BATCH_SIZE
                file_chunks = []
                current_chunk = []
                processed_records_in_file = 0
                
                # Leggi il file e crea i chunk (solo per questo file)
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                record = json.loads(line)
                                current_chunk.append((record, file_name, category))  # Aggiungi categoria
                                processed_records_in_file += 1
                                
                                # Usa BATCH_SIZE dinamico invece di chunk_size fisso
                                if len(current_chunk) >= BATCH_SIZE:  # Era chunk_size, ora BATCH_SIZE (75k)
                                    file_chunks.append((current_chunk, file_name, batch_id, current_table_definitions, category))
                                    current_chunk = []
                            except Exception as e:
                                log_error_with_context(import_logger, e, "parsing record", file_name)
                                continue
                                
                    if current_chunk:
                        file_chunks.append((current_chunk, file_name, batch_id, current_table_definitions, category))
                    
                    batch_logger.info(f"Chunk da processare: {len(file_chunks)} (record effettivi: {processed_records_in_file:,})")
                    
                    # Processa i chunk di questo file SEQUENZIALMENTE
                    records_completed_in_file = 0
                    if file_chunks:
                        for chunk_idx, chunk_data in enumerate(file_chunks, 1):
                            try:
                                chunk_records = len(chunk_data[0])
                                batch_logger.info(f"Processing chunk {chunk_idx}/{len(file_chunks)} ({chunk_records:,} record) per categoria '{category}'")
                                
                                process_chunk_sequential(chunk_data)
                                records_completed_in_file += chunk_records
                                
                                # Aggiorna progresso del file ogni chunk
                                progress_tracker.update_file_progress(records_completed_in_file)
                                
                            except Exception as e:
                                log_error_with_context(batch_logger, e, f"chunk {chunk_idx}", file_name)
                                # Continua con il prossimo chunk invece di fallire tutto
                                continue
                        
                        # Marca il file come processato
                        with DatabaseManager.get_pooled_connection() as conn_mark:
                            mark_file_processed(conn_mark, file_name, processed_records_in_file)
                        
                        # Completa il tracking del file
                        progress_tracker.finish_file(processed_records_in_file)
                        
                        total_processed_files += 1
                        total_processed_records += processed_records_in_file
                    
                    # Libera la memoria dei chunk di questo file
                    del file_chunks
                    del current_chunk
                    gc.collect()
                
                except Exception as e:
                    error_message = str(e)
                    log_error_with_context(import_logger, e, "processing file", file_name)
                    with DatabaseManager.get_pooled_connection() as conn_mark:
                        mark_file_processed(conn_mark, file_name, processed_records_in_file, 'failed', error_message)
        
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
    import sys
    
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
        log_error_with_context(logger, e, "main", "importazione MySQL")
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

def analyze_json_structure_by_category(json_files_by_category):
    """
    Analizza la struttura JSON separatamente per ogni categoria.
    Questo crea tabelle ottimizzate con solo i campi presenti in quella categoria.
    """
    all_table_definitions = {}
    
    with LogContext(analysis_logger, "analisi struttura JSON per categoria", categories=len(json_files_by_category)):
        analysis_logger.info("Analisi INTELLIGENTE PER CATEGORIA per:")
        analysis_logger.info("1. Tabelle ottimizzate con solo campi necessari")
        analysis_logger.info("2. Eliminazione colonne sempre vuote")
        analysis_logger.info("3. Schema specifico per ogni tipo di dato")
        analysis_logger.info("4. Massima efficienza storage")
        
        for category, json_files in json_files_by_category.items():
            if not json_files:
                continue
                
            analysis_logger.info(f"[CATEGORIA] Analizzando '{category}' con {len(json_files)} file")
            
            # Analizza solo i file di questa categoria
            category_table_definitions = analyze_single_category(category, json_files)
            
            # Aggiungi al risultato globale con prefisso categoria
            for field, field_type in category_table_definitions.items():
                all_table_definitions[f"{category}_{field}"] = field_type
            
            analysis_logger.info(f"[CATEGORIA] '{category}': {len(category_table_definitions)} campi specifici rilevati")
        
        analysis_logger.info(f"[TOTALE] Campi totali rilevati: {len(all_table_definitions)}")
        analysis_logger.info("="*80)
        
    return all_table_definitions

def analyze_single_category(category, json_files):
    """Analizza la struttura JSON per una singola categoria."""
    field_types = defaultdict(lambda: defaultdict(int))
    field_lengths = defaultdict(int)
    field_sample_values = defaultdict(list)
    
    # USA PARAMETRI CONFIGURABILI PER VELOCITÀ
    MAX_ROWS_PER_FILE = SCHEMA_MAX_ROWS_PER_FILE
    MAX_FILES_PER_CATEGORY = SCHEMA_MAX_FILES_PER_CATEGORY
    MAX_SAMPLES_PER_FIELD = SCHEMA_MAX_SAMPLES_PER_FIELD
    
    process = psutil.Process()
    max_memory_bytes = USABLE_MEMORY_BYTES * 0.9
    
    # Seleziona un sottoinsieme di file se ce ne sono troppi
    files_to_analyze = json_files
    if len(json_files) > MAX_FILES_PER_CATEGORY:
        # Prendi file distribuiti uniformemente attraverso la lista
        step = len(json_files) // MAX_FILES_PER_CATEGORY
        files_to_analyze = [json_files[i] for i in range(0, len(json_files), step)][:MAX_FILES_PER_CATEGORY]
        analysis_logger.info(f"  [SAMPLE] Categoria '{category}': campiono {len(files_to_analyze)} file su {len(json_files)} totali")
    
    analysis_logger.info(f"  [ANALISI] Categoria '{category}' [{SCHEMA_ANALYSIS_MODE.upper()}]: {len(files_to_analyze)} file, max {MAX_ROWS_PER_FILE} righe/file")
    
    # Pattern per identificare i tipi (stesso del sistema principale)
    patterns = {
        # Tipi numerici precisi
        'pure_integer': r'^\d+$',
        'negative_integer': r'^-\d+$',
        'pure_decimal': r'^\d+[.,]\d+$',
        'negative_decimal': r'^-\d+[.,]\d+$',
        'scientific': r'^\d+[.,]?\d*[eE][+-]?\d+$',
        
        # Valori monetari e percentuali
        'monetary_euro': r'^€?\s*\d+([.,]\d{2})?$',
        'monetary_dollar': r'^\$?\s*\d+([.,]\d{2})?$',
        'percentage': r'^\d+([.,]\d+)?%$',
        
        # Date e tempi
        'date_iso': r'^\d{4}-\d{2}-\d{2}$',
        'date_european': r'^\d{2}/\d{2}/\d{4}$',
        'date_american': r'^\d{2}/\d{2}/\d{4}$',
        'date_italian': r'^\d{2}-\d{2}-\d{4}$',
        'datetime_iso': r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',
        'datetime_european': r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',
        'time_hhmm': r'^\d{2}:\d{2}$',
        'time_hhmmss': r'^\d{2}:\d{2}:\d{2}$',
        'timestamp_unix': r'^\d{10,13}$',
        
        # Booleani
        'boolean_it': r'^(si|no|vero|falso)$',
        'boolean_en': r'^(true|false|yes|no|1|0)$',
        
        # Codici specifici
        'fiscal_code': r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$',
        'partita_iva': r'^\d{11}$',
        'cup_code': r'^[A-Z]\d{13}$',
        'cig_code': r'^[A-Z0-9]{10}$',
        'postal_code_it': r'^\d{5}$',
        
        # Email, URL, telefoni
        'email': r'^[^@]+@[^@]+\.[^@]+$',
        'url': r'^https?://',
        'phone_it': r'^(\+39\s?)?(\d{2,4}[\s-]?)?\d{6,10}$',
        
        # Alfanumerici
        'alphanumeric_upper': r'^[A-Z0-9]+$',
        'alphanumeric_mixed': r'^[A-Za-z0-9]+$',
        'code_with_dashes': r'^[A-Z0-9-]+$',
        
        # Testi
        'text_short': r'^.{1,50}$',
        'text_medium': r'^.{51,255}$',
        'text_long': r'^.{256,}$',
    }
    
    def analyze_value_type(value):
        """Analizza un singolo valore."""
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
        
        length = len(value_str)
        if length <= 50:
            return 'text_short'
        elif length <= 255:
            return 'text_medium'
        else:
            return 'text_long'
    
    def determine_mysql_type(field, type_counts, max_length, sample_values):
        """Determina il tipo MySQL per questa categoria."""
        total_values = sum(type_counts.values())
        if total_values == 0:
            return 'VARCHAR(50)'
        
        sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
        primary_type = sorted_types[0][0]
        primary_count = sorted_types[0][1]
        primary_percentage = (primary_count / total_values) * 100
        
        # Logica semplificata per determinare il tipo MySQL
        if primary_percentage >= 90:
            if primary_type in ['integer_native', 'pure_integer', 'negative_integer']:
                return 'INT'
            elif primary_type in ['decimal_native', 'pure_decimal', 'negative_decimal']:
                return 'DECIMAL(20,6)'
            elif primary_type in ['monetary_euro', 'monetary_dollar']:
                return 'DECIMAL(15,2)'
            elif primary_type == 'percentage':
                return 'DECIMAL(5,2)'
            elif primary_type in ['date_iso', 'date_european', 'date_american', 'date_italian']:
                return 'DATE'
            elif primary_type in ['datetime_iso', 'datetime_european', 'timestamp_unix']:
                return 'DATETIME'
            elif primary_type in ['boolean_native', 'boolean_it', 'boolean_en']:
                return 'BOOLEAN'
            elif primary_type == 'fiscal_code':
                return 'VARCHAR(16)'
            elif primary_type == 'partita_iva':
                return 'VARCHAR(11)'
            elif primary_type == 'cup_code':
                return 'VARCHAR(15)'
            elif primary_type == 'cig_code':
                return 'VARCHAR(10)'
            elif primary_type == 'postal_code_it':
                return 'VARCHAR(5)'
            elif primary_type == 'email':
                return 'VARCHAR(255)'
            elif primary_type == 'url':
                return 'TEXT'
            elif primary_type == 'json_native':
                return 'JSON'
        
        # Fallback basato sulla lunghezza
        if max_length <= 50:
            return 'VARCHAR(100)'
        elif max_length <= 255:
            return 'VARCHAR(500)'
        elif max_length <= 1000:
            return 'TEXT'
        else:
            return 'LONGTEXT'
    
    # Analizza i file di questa categoria (VELOCE)
    records_analyzed = 0
    files_analyzed = 0
    
    for json_file in files_to_analyze:  # Usa il sottoinsieme di file
        files_analyzed += 1
        file_records = 0
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if file_records >= MAX_ROWS_PER_FILE:  # LIMITE CONFIGURABILE
                        break
                        
                    try:
                        record = json.loads(line.strip())
                        file_records += 1
                        records_analyzed += 1
                        
                        for field, value in record.items():
                            field = field.lower().replace(' ', '_')
                            
                            value_type = analyze_value_type(value)
                            field_types[field][value_type] += 1
                            
                            # Campioni configurabili per velocità
                            if len(field_sample_values[field]) < MAX_SAMPLES_PER_FIELD:
                                field_sample_values[field].append(value)
                            
                            if isinstance(value, str):
                                current_length = len(value)
                                if current_length > field_lengths[field]:
                                    field_lengths[field] = current_length
                        
                        # Controllo memoria ogni 500 record (era 1000)
                        if file_records % 500 == 0:
                            current_memory = process.memory_info().rss
                            if current_memory > max_memory_bytes:
                                break
                                
                    except Exception as e:
                        continue
                        
        except Exception as e:
            analysis_logger.warning(f"  [WARN] Errore nel file {json_file}: {e}")
            continue
    
    # Crea le definizioni della tabella per questa categoria
    table_definitions = {}
    for field, type_counts in field_types.items():
        mysql_type = determine_mysql_type(field, type_counts, field_lengths[field], field_sample_values[field])
        table_definitions[field] = mysql_type
    
    analysis_logger.info(f"  [RESULT] Categoria '{category}': {records_analyzed:,} record analizzati ({files_analyzed} file), {len(table_definitions)} campi unici")
    
    return table_definitions

def create_dynamic_tables_by_category(conn, category_table_definitions):
    """Crea tabelle dinamiche ottimizzate con schema specifico per ogni categoria."""
    with LogContext(db_logger, "creazione tabelle ottimizzate per categoria", categories=len(category_table_definitions)):
        cursor = conn.cursor()
        
        total_tables_created = 0
        total_fields_created = 0
        
        try:
            # Prima verifica e migra le tabelle esistenti se necessario
            categories = list(category_table_definitions.keys())
            for category in categories:
                table_name = f"{category}_data"
                check_and_update_table_structure(cursor, table_name, {category: True})
            
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
                
                # Crea la tabella ottimizzata
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
                cursor.execute(create_table_sql)
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
                        cursor.execute(create_json_table)
                        json_tables_count += 1
                
                if json_tables_count > 0:
                    db_logger.info(f"[CREATE] {json_tables_count} tabelle JSON specifiche per categoria '{category}'")
                
                # Report per categoria
                db_logger.info(f"[RESULT] Categoria '{category}': tabella ottimizzata con {len(table_definitions)} campi totali")
            
            # Crea tabelle metadati globali
            create_metadata_tables_optimized(cursor, category_table_definitions)
            
            # COMMIT ESPLICITO per assicurarsi che le tabelle siano create prima di iniziare il processing
            conn.commit()
            db_logger.info("[COMMIT] Tutte le tabelle e metadati salvati nel database")
            
            # VERIFICA che le tabelle metadati esistano e contengano dati
            try:
                cursor.execute("SELECT COUNT(*) FROM category_field_mapping")
                mapping_count = cursor.fetchone()[0]
                db_logger.info(f"[VERIFY] Tabella category_field_mapping contiene {mapping_count} record")
                
                cursor.execute("SHOW TABLES LIKE '%_data'")
                data_tables = cursor.fetchall()
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
            conn.rollback()
            raise
        finally:
            cursor.close()

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

if __name__ == "__main__":
    main()
