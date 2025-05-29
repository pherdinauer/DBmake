﻿import os
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
from .database import DatabaseManager
from .utils import (
    LogContext, log_memory_status, log_performance_stats, 
    log_file_progress, log_batch_progress, log_error_with_context,
    log_resource_optimization, check_disk_space
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
JSON_BASE_PATH = os.environ.get('ANAC_BASE_PATH', '/database/JSON')
BATCH_SIZE = int(os.environ.get('IMPORT_BATCH_SIZE', 75000))  # Aumentato da 25k a 75k

# Ottimizzazione DINAMICA basata sulle risorse del sistema
CPU_CORES = multiprocessing.cpu_count()
NUM_THREADS = max(4, CPU_CORES - 1)  # Usa tutti i core meno 1, minimo 4
NUM_WORKERS = 1   # MONO-PROCESSO per evitare conflitti MySQL, ma thread aggressivi
logger.info(f"🖥️  CPU cores disponibili: {CPU_CORES}")
logger.info(f"🚀  Thread per elaborazione: {NUM_THREADS} (dinamico: {CPU_CORES}-1)")
logger.info(f"🖥️  Worker process: {NUM_WORKERS} (MONO-PROCESSO per stabilità)")
logger.info(f"🚀  Batch size: {BATCH_SIZE:,} (aumentato per sfruttare RAM)")

# Calcola la RAM totale del sistema - aggressivo ma sicuro
TOTAL_MEMORY_BYTES = psutil.virtual_memory().total
TOTAL_MEMORY_GB = TOTAL_MEMORY_BYTES / (1024 ** 3)
MEMORY_BUFFER_RATIO = 0.2  # Solo 20% libero, 80% usabile
USABLE_MEMORY_BYTES = int(TOTAL_MEMORY_BYTES * (1 - MEMORY_BUFFER_RATIO))
USABLE_MEMORY_GB = USABLE_MEMORY_BYTES / (1024 ** 3)

# Chunk size più aggressivo con più RAM
CHUNK_SIZE_INIT_RATIO = 0.10   # 10% della RAM usabile (raddoppiato)
CHUNK_SIZE_MAX_RATIO = 0.25    # 25% della RAM usabile (aumentato)
AVG_RECORD_SIZE_BYTES = 2 * 1024  # Stimiamo 2KB per record
INITIAL_CHUNK_SIZE = max(5000, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_INIT_RATIO) / AVG_RECORD_SIZE_BYTES))
MAX_CHUNK_SIZE = max(INITIAL_CHUNK_SIZE, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_MAX_RATIO) / AVG_RECORD_SIZE_BYTES))
MIN_CHUNK_SIZE = 5000  # Aumentato da 1000 a 5000

# Chunk size massimo MOLTO più aggressivo
MAX_CHUNK_SIZE = min(MAX_CHUNK_SIZE, 150000)  # Aumentato da 75k a 150k

def log_memory_status(logger_instance: logging.Logger, context: str = "") -> None:
    """Helper per logging status memoria."""
    memory_info = psutil.virtual_memory()
    used_gb = memory_info.used / (1024**3)
    total_gb = memory_info.total / (1024**3)
    usage_pct = memory_info.percent
    available_gb = memory_info.available / (1024**3)
    
    prefix = f"[{context}] " if context else ""
    logger_instance.info(f"💻 {prefix}RAM: {used_gb:.1f}GB/{total_gb:.1f}GB ({usage_pct:.1f}%) | Disponibile: {available_gb:.1f}GB")

def log_performance_stats(logger_instance: logging.Logger, operation: str, count: int, elapsed_time: float, context: str = "") -> None:
    """Helper per logging statistiche performance."""
    speed = count / elapsed_time if elapsed_time > 0 else 0
    prefix = f"[{context}] " if context else ""
    logger_instance.info(f"📊 {prefix}{operation}: {count:,} elementi in {elapsed_time:.1f}s ({speed:.1f} el/s)")

def log_file_progress(logger_instance: logging.Logger, current: int, total: int, file_name: str = "", extra_info: str = "") -> None:
    """Helper per logging progresso file."""
    pct = (current / total * 100) if total > 0 else 0
    file_info = f" - {file_name}" if file_name else ""
    extra = f" | {extra_info}" if extra_info else ""
    logger_instance.info(f"📁 Progresso: {current}/{total} ({pct:.1f}%){file_info}{extra}")

def log_batch_progress(logger_instance: logging.Logger, processed: int, total: int, speed: Optional[float] = None, memory_info: Optional[str] = None) -> None:
    """Helper per logging progresso batch con informazioni opzionali."""
    pct = (processed / total * 100) if total > 0 else 0
    speed_info = f" | {speed:.0f} rec/s" if speed else ""
    memory_info_str = f" | RAM: {memory_info}" if memory_info else ""
    logger_instance.info(f"📦 Batch: {processed:,}/{total:,} ({pct:.1f}%){speed_info}{memory_info_str}")

def log_error_with_context(logger_instance: logging.Logger, error: Exception, context: str = "", operation: str = "") -> None:
    """Helper per logging errori con contesto."""
    context_str = f"[{context}] " if context else ""
    operation_str = f" durante {operation}" if operation else ""
    logger_instance.error(f"❌ {context_str}Errore{operation_str}: {error}")

def log_resource_optimization(logger_instance: logging.Logger) -> None:
    """Helper per logging configurazione risorse ottimizzate."""
    logger_instance.info("🚀 Configurazione risorse DINAMICHE ottimizzate:")
    logger_instance.info(f"   💻 CPU: {CPU_CORES} core → {NUM_THREADS} thread attivi ({(NUM_THREADS/CPU_CORES*100):.0f}% utilizzo)")
    logger_instance.info(f"   🖥️ RAM totale: {TOTAL_MEMORY_GB:.1f}GB")
    logger_instance.info(f"   🚀 RAM usabile: {USABLE_MEMORY_GB:.1f}GB (buffer {MEMORY_BUFFER_RATIO*100:.0f}%)")
    logger_instance.info(f"   🔥 Worker process: {NUM_WORKERS} (MONO-PROCESSO + thread aggressivi)")
    logger_instance.info(f"   📦 Batch size principale: {BATCH_SIZE:,}")
    
    current_insert_batch = calculate_dynamic_insert_batch_size()
    current_ram = psutil.virtual_memory().available / (1024**3)
    logger_instance.info(f"   ⚡ INSERT batch dinamico: {current_insert_batch:,} (RAM disponibile: {current_ram:.1f}GB)")
    logger_instance.info(f"   🎯 Chunk size max: {MAX_CHUNK_SIZE:,}")

def handle_data_too_long_error(cursor, error_message, table_name):
    """Gestisce errori di 'Data too long' modificando la colonna da VARCHAR a TEXT."""
    # Estrai il nome della colonna dall'errore
    match = re.search(r"Data too long for column '([^']+)'", error_message)
    if not match:
        return False
        
    column_name = match.group(1)
    logger.warning(f"⚠️ Colonna '{column_name}' troppo piccola, converto a TEXT...")
    
    try:
        # Modifica la colonna da VARCHAR a TEXT
        alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} TEXT"
        cursor.execute(alter_query)
        logger.info(f"✅ Colonna '{column_name}' convertita a TEXT")
        return True
    except Exception as e:
        logger.error(f"❌ Errore nella modifica della colonna '{column_name}': {e}")
        return False

class AdaptiveBatchSizer:
    """Gestisce il batch size adattivo basato sull'utilizzo RAM real-time."""
    
    def __init__(self, initial_batch_size, target_ram_usage=0.8):
        self.current_batch_size = initial_batch_size
        self.target_ram_usage = target_ram_usage
        self.performance_history = []
        self.adjustment_factor = 2.0  # Moltiplicatore MOLTO AGGRESSIVO (era 1.5)
        self.min_batch = 10000  # Aumentato da 5000
        self.max_batch = 1000000  # 🚀🚀🚀 UN MILIONE! (era 300000)
        
    def adjust_batch_size(self, current_ram_usage, processing_speed):
        """Aggiusta il batch size basato su utilizzo RAM e performance."""
        self.performance_history.append((current_ram_usage, processing_speed))
        
        # Se abbiamo abbastanza storia, adatta il batch size
        if len(self.performance_history) >= 1:  # Reagisce IMMEDIATAMENTE
            avg_ram_usage = current_ram_usage  # Usa valore corrente, non media
            
            # Se RAM MOLTO sottoutilizzata (< 60% del target), aumenta DRASTICAMENTE
            if avg_ram_usage < (self.target_ram_usage * 0.60):
                new_batch_size = min(self.max_batch, int(self.current_batch_size * 3.0))  # TRIPLICA!
                if new_batch_size > self.current_batch_size:
                    logger.info(f"🚀🚀🚀 MEGA AUTO-TUNE: Batch size TRIPLICATO da {self.current_batch_size:,} a {new_batch_size:,} "
                               f"(RAM SPRECATA: {avg_ram_usage*100:.1f}% vs target {self.target_ram_usage*100:.0f}%)")
                    self.current_batch_size = new_batch_size
                    
            # Se RAM sottoutilizzata (< 75% del target), aumenta aggressivamente
            elif avg_ram_usage < (self.target_ram_usage * 0.75):
                new_batch_size = min(self.max_batch, int(self.current_batch_size * self.adjustment_factor))
                if new_batch_size > self.current_batch_size:
                    logger.info(f"🚀🚀 SUPER AUTO-TUNE: Batch size RADDOPPIATO da {self.current_batch_size:,} a {new_batch_size:,} "
                               f"(RAM sottoutilizzata: {avg_ram_usage*100:.1f}%)")
                    self.current_batch_size = new_batch_size
                    
            # Se RAM eccessivamente utilizzata (> 90% del target), diminuisci
            elif avg_ram_usage > (self.target_ram_usage * 0.90):
                new_batch_size = max(self.min_batch, int(self.current_batch_size / 1.5))
                if new_batch_size < self.current_batch_size:
                    logger.warning(f"⚠️ AUTO-TUNE: Batch size ridotto da {self.current_batch_size:,} a {new_batch_size:,} "
                                  f"(RAM sovraccarica: {avg_ram_usage*100:.1f}%)")
                    self.current_batch_size = new_batch_size
        
        return self.current_batch_size

def calculate_dynamic_insert_batch_size():
    """Calcola dinamicamente il batch size per INSERT basato su utilizzo RAM MASSIMO (85-90%)."""
    # Memoria totale e utilizzata
    memory_info = psutil.virtual_memory()
    total_memory = memory_info.total
    available_memory = memory_info.available
    used_memory = memory_info.used
    
    # Target MASSIMO: utilizzare fino al 90% della RAM totale (10% buffer di sicurezza)
    target_memory_usage = total_memory * 0.90  # Era 0.85, ora 0.90!
    current_usage_pct = used_memory / total_memory
    available_for_batch = target_memory_usage - used_memory
    
    # Stima grossolana: 1 record = ~2KB in memoria durante elaborazione
    estimated_record_size_bytes = 2 * 1024
    
    # Calcola batch size MASSIMO AGGRESSIVO basato su memoria disponibile
    if available_for_batch > 0:
        max_batch_from_memory = int(available_for_batch / estimated_record_size_bytes)
        
        # Limiti di sicurezza ESTREMI
        min_batch = 10000   # Aumentato da 5000
        max_batch = 1000000  # 🚀🚀🚀 UN MILIONE! (era 300000)
        
        # Batch size ESTREMO basato su utilizzo attuale
        if current_usage_pct < 0.3:      # < 30% RAM usata → batch MOSTRUOSO
            batch_size = min(max_batch, max(500000, max_batch_from_memory))
        elif current_usage_pct < 0.5:   # 30-50% RAM usata → batch gigantesco
            batch_size = min(800000, max(400000, max_batch_from_memory // 2))
        elif current_usage_pct < 0.65:  # 50-65% RAM usata → batch massiccio
            batch_size = min(600000, max(300000, max_batch_from_memory // 3))
        elif current_usage_pct < 0.75:  # 65-75% RAM usata → batch molto alto
            batch_size = min(400000, max(200000, max_batch_from_memory // 4))
        else:                            # > 75% RAM usata → batch alto
            batch_size = min(200000, max(min_batch, max_batch_from_memory // 6))
            
        return max(min_batch, min(batch_size, max_batch))
    else:
        # Se già oltre il 90%, usa batch ridotto ma ancora alto
        return 100000  # Era 50000, ora 100000

def insert_batch_direct(cursor, main_data, table_name, fields):
    """Inserisce i dati direttamente in MySQL senza passare per CSV."""
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
    
    logger.info(f"📥 Inserimento diretto di {len(main_data):,} record in {table_name}")
    logger.info(f"🚀 Batch size AGGRESSIVO iniziale: {initial_batch_size:,}")
    logger.info(f"💻 RAM: {used_ram_gb:.1f}GB/{total_ram_gb:.1f}GB utilizzata ({usage_pct:.1f}%) | "
               f"Target: {target_usage_pct:.0f}% | Disponibile: {available_ram_gb:.1f}GB")
    
    rows_inserted = 0
    
    # Prepara la query INSERT
    placeholders = ', '.join(['%s'] * len(fields))
    insert_query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
    
    insert_start_time = time.time()
    
    try:
        # Processa i dati in batch con dimensioni adattive
        i = 0
        while i < len(main_data):
            # Ottieni il batch size corrente (può essere cambiato dall'adaptive sizer)
            current_batch_size = batch_sizer.current_batch_size
            
            batch_start_time = time.time()
            batch = main_data[i:i + current_batch_size]
            
            try:
                # Esegui INSERT batch
                cursor.executemany(insert_query, batch)
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
                    
                    logger.info(f"🚀 INSERT progresso: {rows_inserted:,}/{len(main_data):,} righe | "
                              f"Batch: {batch_speed:.0f} rec/s | Media: {avg_speed:.0f} rec/s | "
                              f"RAM: {current_ram:.1f}GB ({current_usage:.1f}%) | Batch size: {current_batch_size:,}")
            
            except mysql.connector.Error as e:
                if e.errno == 1406:  # Data too long error
                    logger.warning(f"⚠️ Errore Data too long rilevato: {e}")
                    
                    # Tenta di risolvere modificando la colonna
                    if handle_data_too_long_error(cursor, str(e), table_name):
                        logger.info(f"🔄 Ritento l'inserimento del batch dopo la modifica...")
                        # Riprova l'inserimento dello stesso batch
                        cursor.executemany(insert_query, batch)
                        rows_inserted += len(batch)
                        logger.info(f"✅ Batch inserito con successo dopo la modifica")
                    else:
                        logger.error(f"❌ Impossibile risolvere l'errore Data too long")
                        raise
                else:
                    # Altri errori MySQL
                    raise
            
            i += current_batch_size
        
        total_time = time.time() - insert_start_time
        avg_speed = rows_inserted / total_time if total_time > 0 else 0
        final_ram_gb = psutil.virtual_memory().used / (1024**3)
        final_usage_pct = psutil.virtual_memory().percent
        final_batch_size = batch_sizer.current_batch_size
        
        logger.info(f"🎯 INSERT diretto completato: {rows_inserted:,} righe totali")
        logger.info(f"📊 Performance: {avg_speed:.0f} rec/s in {total_time:.1f}s | "
                   f"Batch size finale: {final_batch_size:,} | RAM finale: {final_ram_gb:.1f}GB ({final_usage_pct:.1f}%)")
        return rows_inserted
        
    except Exception as e:
        logger.error(f"📥 Errore in INSERT diretto: {e}")
        raise

def process_batch(cursor, batch, table_definitions, batch_id, progress_tracker=None):
    if not batch:
        return

    file_name = batch[0][1]
    
    with LogContext(batch_logger, f"processing batch", 
                   batch_size=len(batch), file=file_name, batch_id=batch_id):
        try:
            # Ottieni il mapping dei campi
            try:
                cursor.execute("SELECT original_name, sanitized_name FROM field_mapping")
                field_mapping = dict(cursor.fetchall())
            except mysql.connector.Error as e:
                if e.errno == 2006:  # MySQL server has gone away
                    batch_logger.warning("Connessione persa, riprovo...")
                    raise ValueError("CONNECTION_LOST")
                raise
            
            start_time = time.time()
            
            # Elabora il batch in parallelo
            main_data, json_data = process_batch_parallel(batch, table_definitions, field_mapping, file_name, batch_id)
            
            if main_data:
                # Prepara i campi per INSERT diretto
                fields = ['cig'] + [field_mapping[field] for field in table_definitions.keys() if field.lower() != 'cig'] + ['source_file', 'batch_id']
                
                # Debug: mostra alcuni esempi di dati
                batch_logger.info(f"Dati per INSERT diretto:")
                batch_logger.info(f"   Campi totali: {len(fields)}")
                batch_logger.info(f"   Record totali: {len(main_data):,}")
                
                # Mostra i primi 2 record per debug
                for i, record in enumerate(main_data[:2]):
                    batch_logger.info(f"   Record {i+1}: {len(record)} valori")
                    for j, (field, value) in enumerate(zip(fields, record)):
                        if 'data_' in field.lower() and j < 10:  # Solo primi 10 campi data
                            batch_logger.info(f"     - {field}: {value} ({type(value).__name__})")
                
                # Inserimento diretto in MySQL (no CSV)
                rows_affected = insert_batch_direct(cursor, main_data, 'main_data', fields)
            
            # Gestisci i dati JSON separatamente (manteniamo l'approccio precedente per i JSON)
            if json_data:
                def insert_json_data(field, data):
                    try:
                        json_data_with_metadata = [(cig, json_str, file_name, batch_id) for cig, json_str in data]
                        insert_json = f"""
                        INSERT INTO {field}_data (cig, {field}_json, source_file, batch_id)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            {field}_json = VALUES({field}_json),
                            source_file = VALUES(source_file),
                            batch_id = VALUES(batch_id)
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
            log_performance_stats(batch_logger, "Batch completato", len(batch), elapsed_time)
            
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
    Wrapper per compatibilità con codice esistente.
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
        logger.info(f"   📁 {database_path}: {database_free_gb:.1f}GB")
        logger.info(f"   📁 {tmp_path}: {tmp_free_gb:.1f}GB")
        logger.info(f"   📁 {tmp_dir_path}: {tmp_dir_free_gb:.1f}GB")
        
        # Avvisa se lo spazio è basso
        if database_free_gb < 10:
            logger.warning(f"⚠️ Spazio disponibile su {database_path} è basso!")
        if tmp_free_gb < 1:
            logger.warning(f"⚠️ Spazio disponibile su {tmp_path} è basso!")
        if tmp_dir_free_gb < 1:
            logger.warning(f"⚠️ Spazio disponibile su {tmp_dir_path} è basso!")
            
        return database_free_gb, tmp_free_gb, tmp_dir_free_gb
    except Exception as e:
        logger.error(f"❌ Errore nel controllo dello spazio disco: {e}")
        return None, None, None

def analyze_json_structure(json_files):
    field_types = defaultdict(lambda: defaultdict(int))
    field_lengths = defaultdict(int)
    field_patterns = defaultdict(set)  # Per memorizzare i pattern dei valori
    field_has_mixed = defaultdict(bool)  # Traccia se un campo ha valori misti
    
    with LogContext(analysis_logger, "analisi struttura JSON", files=len(json_files)):
        analysis_logger.info("Analisi per determinare:")
        analysis_logger.info("1. I tipi di dati per ogni campo")
        analysis_logger.info("2. Le lunghezze massime dei campi stringa")
        analysis_logger.info("3. I pattern dei valori per determinare il tipo più appropriato")
        analysis_logger.info("4. Limite: 2k righe per file + controllo memoria dinamico")
        analysis_logger.info("5. Logica CONSERVATIVA: anche un solo valore alfanumerico = VARCHAR")
        
        total_files = len(json_files)
        files_analyzed = 0
        records_analyzed = 0
        start_time = time.time()
        last_progress_time = time.time()
        progress_interval = 1.0
        
        # Limite righe per file per analisi veloce
        MAX_ROWS_PER_FILE = 2000
        
        # Monitoraggio memoria dinamico come failsafe
        process = psutil.Process()
        max_memory_bytes = USABLE_MEMORY_BYTES * 0.9  # 90% del buffer disponibile per l'analisi
        
        analysis_logger.info(f"Limite righe per file: {MAX_ROWS_PER_FILE:,}")
        log_memory_status(analysis_logger, "limite massimo analisi")
        
        # Campi che devono SEMPRE essere VARCHAR (blacklist)
        ALWAYS_VARCHAR_FIELDS = {
            'numero_gara', 'codice_gara', 'id_gara', 'numero', 'codice', 'id', 
            'identificativo', 'riferimento', 'cup', 'cig', 'numero_lotto',
            'codice_fiscale', 'partita_iva', 'numero_verde', 'numero_telefono'
        }
        
        # Campi che devono SEMPRE essere DATE/DATETIME (whitelist)
        ALWAYS_DATE_FIELDS = {
            'data_creazione', 'data_pubblicazione', 'data_scadenza', 'data_aggiornamento',
            'data_inizio', 'data_fine', 'data_inserimento', 'data_modifica',
            'created_at', 'updated_at', 'published_at', 'expired_at'
        }
        
        # Pattern migliorati per identificare meglio i tipi di campi
        patterns = {
            'monetary': r'^[€$]?\s*\d+([.,]\d{2})?$',  # Valori monetari
            'percentage': r'^\d+([.,]\d+)?%$',         # Percentuali
            'date_iso': r'^\d{4}-\d{2}-\d{2}$',       # Date ISO (YYYY-MM-DD)
            'date_european': r'^\d{2}/\d{2}/\d{4}$',  # Date europee (DD/MM/YYYY)
            'date_american': r'^\d{2}/\d{2}/\d{4}$',  # Date americane (MM/DD/YYYY)
            'datetime_iso': r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',  # DateTime ISO
            'datetime_european': r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',  # DateTime europeo
            'timestamp': r'^\d{10,13}$',               # Timestamp Unix
            'boolean': r'^(true|false|yes|no|si|no|1|0)$',  # Booleani
            'pure_integer': r'^\d+$',                  # SOLO numeri interi puri
            'pure_decimal': r'^\d+[.,]\d+$',          # SOLO decimali puri
            'alphanumeric_mixed': r'^[A-Z0-9]+$',     # Alfanumerici misti (es. Z2B1FADD05)
            'email': r'^[^@]+@[^@]+\.[^@]+$',         # Email
            'url': r'^https?://',                      # URL
            'phone': r'^\+?\d{8,15}$',                # Numeri di telefono
            'postal_code': r'^\d{5}$',                 # CAP
            'fiscal_code': r'^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]\d{2}$',  # Codice fiscale
            'partita_iva': r'^\d{11}$',               # Partita IVA
            'cup_code': r'^[A-Z]\d{2}[A-Z]\d{2}[A-Z]\d{2}[A-Z]\d{2}[A-Z]\d{2}[A-Z]\d{2}$',  # CUP
            'cig_code': r'^[A-Z]\d{8}[A-Z]\d{2}$'    # CIG
        }
        
        def get_value_pattern_and_type(value):
            """Determina il pattern di un valore e se è misto."""
            if value is None:
                return 'null', False
            if isinstance(value, bool):
                return 'boolean', False
            if isinstance(value, int):
                return 'pure_integer', False
            if isinstance(value, float):
                return 'pure_decimal', False
            if isinstance(value, (list, dict)):
                return 'json', False
                
            value_str = str(value).strip()
            if not value_str:
                return 'empty', False
                
            # Controlla se contiene sia lettere che numeri (MISTO)
            has_letters = any(c.isalpha() for c in value_str)
            has_digits = any(c.isdigit() for c in value_str)
            is_mixed = has_letters and has_digits
            
            for pattern_name, pattern in patterns.items():
                if re.match(pattern, value_str, re.IGNORECASE):
                    return pattern_name, is_mixed
            return 'text', has_letters or has_digits
        
        def check_memory_limit():
            """Controlla se abbiamo raggiunto il limite di memoria."""
            current_memory = process.memory_info().rss
            return current_memory < max_memory_bytes
        
        def determine_field_type(field, patterns, field_has_mixed_values, values_count):
            """Determina il tipo più appropriato per un campo basato sui suoi pattern."""
            field_lower = field.lower()
            
            # REGOLA 0: Campi data devono SEMPRE essere DATETIME (priorità massima)
            if field_lower in ALWAYS_DATE_FIELDS:
                return 'DATETIME'
            
            # REGOLA 0.5: Campi lunghi comuni sempre TEXT (denominazioni, descrizioni, etc.)
            ALWAYS_TEXT_KEYWORDS = [
                'denominazione', 'descrizione', 'amministrazione', 'ragione_sociale', 
                'oggetto', 'dettaglio', 'motivazione', 'specifiche', 'note'
            ]
            if any(keyword in field_lower for keyword in ALWAYS_TEXT_KEYWORDS):
                return 'TEXT'
            
            # REGOLA 1: Campi nella blacklist sono SEMPRE VARCHAR (ma con dimensioni ottimizzate)
            if field_lower in ALWAYS_VARCHAR_FIELDS:
                # Dimensioni specifiche per campi noti
                if field_lower in ['cig']:
                    return 'VARCHAR(13)'
                elif field_lower in ['cup']:
                    return 'VARCHAR(15)'
                elif field_lower in ['codice_fiscale']:
                    return 'VARCHAR(16)'
                elif field_lower in ['partita_iva']:
                    return 'VARCHAR(11)'
                elif field_lower in ['numero_verde', 'numero_telefono']:
                    return 'VARCHAR(20)'
                else:
                    # Per altri campi della blacklist, usa dimensione basata sulla lunghezza effettiva
                    max_length = field_lengths.get(field, 50)
                    if max_length > 1000:
                        return 'TEXT'
                    elif max_length > 500:
                        return 'VARCHAR(500)'
                    elif max_length > 100:
                        return 'VARCHAR(150)'
                    else:
                        return 'VARCHAR(100)'
                
            # REGOLA 2: Se il campo ha valori misti alfanumerici, è VARCHAR
            if field_has_mixed_values:
                max_length = field_lengths.get(field, 50)
                if max_length > 1000:
                    return 'TEXT'
                elif max_length > 500:
                    return 'VARCHAR(500)'
                elif max_length > 100:
                    return 'VARCHAR(150)'
                else:
                    return 'VARCHAR(100)'
                
            # REGOLA 3: Se contiene pattern alfanumerici misti, è VARCHAR
            if 'alphanumeric_mixed' in patterns:
                max_length = field_lengths.get(field, 50)
                if max_length > 1000:
                    return 'TEXT'
                elif max_length > 500:
                    return 'VARCHAR(500)'
                elif max_length > 100:
                    return 'VARCHAR(150)'
                else:
                    return 'VARCHAR(100)'
                
            # Se il campo è vuoto o ha solo valori null, usa VARCHAR piccolo
            if not patterns or patterns == {'null'} or patterns == {'empty'}:
                return 'VARCHAR(50)'
                
            # Se il campo contiene JSON, usa il tipo JSON
            if 'json' in patterns:
                return 'JSON'
                
            # Se il campo contiene booleani, usa BOOLEAN
            if patterns == {'boolean'}:
                return 'BOOLEAN'
                
            # Se il campo contiene solo numeri interi PURI, usa INT
            if patterns == {'pure_integer'}:
                return 'INT'
                
            # Se il campo contiene numeri decimali PURI o monetari, usa DECIMAL
            if patterns == {'pure_decimal'} or patterns == {'monetary'}:
                return 'DECIMAL(20,2)'
                
            # Se il campo contiene date, usa DATE o DATETIME
            if any(p in patterns for p in ['date_iso', 'date_european', 'date_american']):
                return 'DATE'
                
            # Se il campo contiene datetime o timestamp, usa DATETIME
            if any(p in patterns for p in ['datetime_iso', 'datetime_european', 'timestamp']):
                return 'DATETIME'
                
            # Se il campo contiene percentuali, usa DECIMAL
            if patterns == {'percentage'}:
                return 'DECIMAL(5,2)'
                
            # Se il campo contiene email, usa VARCHAR
            if patterns == {'email'}:
                return 'VARCHAR(100)'
                
            # Se il campo contiene URL, usa TEXT (spesso lunghi)
            if patterns == {'url'}:
                return 'TEXT'
                
            # Se il campo contiene numeri di telefono, usa VARCHAR
            if patterns == {'phone'}:
                return 'VARCHAR(20)'
                
            # Se il campo contiene CAP, usa VARCHAR
            if patterns == {'postal_code'}:
                return 'VARCHAR(5)'
                
            # Se il campo contiene codice fiscale, usa VARCHAR
            if patterns == {'fiscal_code'}:
                return 'VARCHAR(16)'
                
            # Se il campo contiene partita IVA, usa VARCHAR
            if patterns == {'partita_iva'}:
                return 'VARCHAR(11)'
                
            # Se il campo contiene CUP, usa VARCHAR
            if patterns == {'cup_code'}:
                return 'VARCHAR(15)'
                
            # Se il campo contiene CIG, usa VARCHAR
            if patterns == {'cig_code'}:
                return 'VARCHAR(13)'
                
            # FALLBACK: Per sicurezza, usa VARCHAR con dimensione basata sulla lunghezza effettiva
            max_length = field_lengths.get(field, 50)
            if max_length > 1000:
                return 'TEXT'
            elif max_length > 500:
                return 'VARCHAR(500)'
            elif max_length > 200:
                return 'VARCHAR(250)'
            elif max_length > 100:
                return 'VARCHAR(150)'
            elif max_length > 50:
                return 'VARCHAR(100)'
            else:
                return 'VARCHAR(50)'
        
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
                            # Limite principale: 2k righe per file
                            if file_records >= MAX_ROWS_PER_FILE:
                                limit_reached = True
                                break
                                
                            record = json.loads(line.strip())
                            file_records += 1
                            records_analyzed += 1
                            
                            for field, value in record.items():
                                if value is None:
                                    continue
                                    
                                field = field.lower().replace(' ', '_')
                                pattern, is_mixed = get_value_pattern_and_type(value)
                                field_patterns[field].add(pattern)
                                
                                # Traccia se il campo ha valori misti
                                if is_mixed:
                                    field_has_mixed[field] = True
                                
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
                status = "🔥 LIMITE RAM" if memory_exceeded else "📋 LIMITE 2K" if limit_reached else "✅"
                    
                log_performance_stats(analysis_logger, f"File {status}", file_records, file_time, 
                                    f"{files_analyzed}/{total_files}")
                
                # Garbage collection dopo ogni file
                gc.collect()
                
            except Exception as e:
                log_error_with_context(analysis_logger, e, f"file {json_file}", "analisi")
                continue
        
        # Crea le definizioni delle tabelle
        table_definitions = {}
        total_estimated_row_size = 0
        field_size_breakdown = []
        
        for field, patterns in field_patterns.items():
            column_def = determine_field_type(field, patterns, field_has_mixed[field], records_analyzed)
            table_definitions[field] = column_def
            
            # Calcola la dimensione stimata del campo
            field_size = 0
            if column_def.startswith('VARCHAR'):
                # Estrai la dimensione dal VARCHAR
                size_str = column_def[8:-1]  # Rimuove 'VARCHAR(' e ')'
                field_size = int(size_str) * 3  # UTF8MB4 usa max 3 bytes per carattere
            elif column_def == 'TEXT':
                field_size = 768  # Dimensione minima per TEXT in InnoDB
            elif column_def == 'INT':
                field_size = 4
            elif column_def.startswith('DECIMAL'):
                field_size = 8
            elif column_def == 'DATE':
                field_size = 3
            elif column_def == 'DATETIME':
                field_size = 8
            elif column_def == 'BOOLEAN':
                field_size = 1
            elif column_def == 'JSON':
                field_size = 0  # I campi JSON vanno in tabelle separate
            else:
                field_size = 10  # Default per tipi sconosciuti
                
            total_estimated_row_size += field_size
            if field_size > 0:
                field_size_breakdown.append((field, column_def, field_size))
        
        # Garbage collection finale
        gc.collect()
        
        # Stampa un riepilogo della struttura trovata
        mysql_row_limit = 65535
        
        analysis_logger.info("Riepilogo analisi:")
        log_file_progress(analysis_logger, files_analyzed, total_files, "completati")
        log_performance_stats(analysis_logger, "Record analizzati", records_analyzed, time.time() - start_time)
        analysis_logger.info(f"📊 Media record per file: {records_analyzed/files_analyzed:.0f}")
        analysis_logger.info(f"📊 Campi trovati: {len(table_definitions)}")
        analysis_logger.info(f"📊 Campi misti trovati: {sum(1 for v in field_has_mixed.values() if v)}")
        log_memory_status(memory_logger, "finale analisi")
        
        # Riepilogo ottimizzazioni dimensioni
        analysis_logger.info(f"Ottimizzazioni dimensioni tabella:")
        analysis_logger.info(f"   📊 Dimensione riga stimata: {total_estimated_row_size:,} bytes")
        analysis_logger.info(f"   📊 Limite MySQL InnoDB: {mysql_row_limit:,} bytes")
        
        if total_estimated_row_size > mysql_row_limit:
            analysis_logger.warning(f"⚠️ RIGA TROPPO GRANDE! Supera il limite di {(total_estimated_row_size - mysql_row_limit):,} bytes")
            analysis_logger.warning(f"⚠️ Convertendo più campi a TEXT...")
            
            # Riottimizza convertendo i campi più grandi a TEXT
            for field, column_def in table_definitions.items():
                if column_def.startswith('VARCHAR') and '500' in column_def:
                    table_definitions[field] = 'TEXT'
                    analysis_logger.info(f"      🔄 {field}: {column_def} → TEXT")
            
            # Ricalcola la dimensione
            total_estimated_row_size = 0
            for field, column_def in table_definitions.items():
                field_size = 0
                if column_def.startswith('VARCHAR'):
                    size_str = column_def[8:-1]
                    field_size = int(size_str) * 3
                elif column_def == 'TEXT':
                    field_size = 768  
                elif column_def == 'INT':
                    field_size = 4
                elif column_def.startswith('DECIMAL'):
                    field_size = 8
                elif column_def == 'DATE':
                    field_size = 3
                elif column_def == 'DATETIME':
                    field_size = 8
                elif column_def == 'BOOLEAN':
                    field_size = 1
                elif column_def == 'JSON':
                    field_size = 0
                else:
                    field_size = 10
                total_estimated_row_size += field_size
            
            analysis_logger.info(f"   ✅ Nuova dimensione riga stimata: {total_estimated_row_size:,} bytes")
        else:
            analysis_logger.info(f"   ✅ Dimensione riga OK ({(mysql_row_limit - total_estimated_row_size):,} bytes di margine)")
        
        # Mostra breakdown dei campi più grandi
        field_size_breakdown.sort(key=lambda x: x[2], reverse=True)
        analysis_logger.info(f"Top 10 campi per dimensione:")
        for i, (field, column_def, size) in enumerate(field_size_breakdown[:10]):
            mixed_indicator = "🔀" if field_has_mixed[field] else ""
            analysis_logger.info(f"   {i+1:2d}. {field}: {column_def} ({size} bytes) {mixed_indicator}")
        
        analysis_logger.info("Dettaglio tutti i campi:")
        for field, def_type in table_definitions.items():
            mixed_indicator = "🔀" if field_has_mixed[field] else ""
            analysis_logger.info(f"   📋 {field}: {def_type} {mixed_indicator}")
            if field in field_patterns:
                analysis_logger.info(f"      Pattern trovati: {', '.join(sorted(field_patterns[field]))}")
        
        return table_definitions

def generate_short_alias(field_name, existing_aliases):
    """Genera un alias corto per un campo lungo."""
    # Rimuovi caratteri speciali e spazi
    base = ''.join(c for c in field_name if c.isalnum())
    # Prendi le prime lettere di ogni parola
    words = base.split('_')
    if len(words) > 1:
        # Se ci sono più parole, usa le iniziali
        alias = ''.join(word[0] for word in words if word)
    else:
        # Altrimenti usa i primi caratteri
        alias = base[:8]
    
    # Aggiungi un numero se l'alias esiste già
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
    
    # Se il nome è troppo lungo, genera un alias
    if len(sanitized) > 64:
        short_alias = generate_short_alias(sanitized, existing_aliases)
        existing_aliases.add(short_alias)
        return short_alias
    
    return sanitized

def get_column_type(field_type, length):
    """Determina il tipo di colonna appropriato in base alla lunghezza."""
    if field_type == 'VARCHAR':
        # Se la lunghezza è maggiore di 1000, usa TEXT
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
                db_logger.info(f"   📋 {field_name}: {field_type}")
                
                # Evidenzia i campi data
                if 'data_' in field_name.lower() or field_name.lower() in ['created_at', 'updated_at']:
                    date_columns.append((field_name, field_type))
            
            if date_columns:
                db_logger.info(f"Campi data nella tabella:")
                for field_name, field_type in date_columns:
                    status = "✅" if field_type.upper() in ['DATE', 'DATETIME', 'TIMESTAMP'] else "⚠️"
                    db_logger.info(f"   {status} {field_name}: {field_type}")
                    
        except mysql.connector.Error as e:
            log_error_with_context(db_logger, e, table_name, "verifica struttura")
        finally:
            cursor.close()

def create_main_table(cursor, table_definitions, field_mapping, column_types):
    """Crea la tabella principale con tutti i campi non-JSON."""
    with LogContext(db_logger, "creazione tabella principale"):
        # Escludi il campo 'cig' dalla lista dei campi normali poiché è già la chiave primaria
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
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    sanitized_name = VALUES(sanitized_name),
                    field_type = VALUES(field_type)
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

def create_dynamic_tables(conn, table_definitions):
    """Crea tutte le tabelle dinamiche necessarie per l'importazione."""
    with LogContext(db_logger, "creazione tabelle dinamiche", tables=len(table_definitions)):
        cursor = conn.cursor()
        
        try:
            # 1. Prepara mapping dei campi e tipi di colonna
            field_mapping, column_types = prepare_field_mappings(table_definitions)
            
            # 2. Crea la tabella principale
            create_main_table(cursor, table_definitions, field_mapping, column_types)
            
            # 3. Verifica la struttura creata
            verify_table_structure(conn)
            
            # 4. Crea tabelle separate per i campi JSON
            json_tables_count = create_json_tables(cursor, table_definitions, field_mapping)
            
            # 5. Crea tabelle di metadati
            create_metadata_tables(cursor, table_definitions, field_mapping, column_types)
            
            db_logger.info(f"Creazione completata: 1 tabella principale, {json_tables_count} tabelle JSON, 2 tabelle metadati")
            
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
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                processed_at = CURRENT_TIMESTAMP,
                record_count = VALUES(record_count),
                status = VALUES(status),
                error_message = VALUES(error_message)
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

def process_chunk_unified(args, execution_mode="sequential"):
    """
    Processa un chunk con logica unificata per multiprocessing e sequential.
    
    Args:
        args: (chunk, file_name, batch_id, table_definitions)
        execution_mode: "multiprocessing" o "sequential"
    """
    chunk, file_name, batch_id, table_definitions = args
    
    # Logging specifico per modalità
    if execution_mode == "multiprocessing":
        print(f"[Multiprocessing] Processo PID={os.getpid()} elabora chunk di {len(chunk)} record del file {file_name}")
    else:
        batch_logger.info(f"[Sequential] Elabora chunk di {len(chunk)} record del file {file_name}")
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            # Usa DatabaseManager per ottenere connessione dal pool
            with DatabaseManager.get_pooled_connection() as conn:
                cursor = conn.cursor()
                try:
                    process_batch(cursor, chunk, table_definitions, batch_id)
                    conn.commit()
                    return  # Successo
                except Exception as e:
                    error_msg = f"Errore nel processare chunk (tentativo {attempt + 1}/{max_retries}): {e}"
                    if execution_mode == "multiprocessing":
                        print(f"❌ {error_msg}")  # In multiprocessing usa print per evitare conflitti logger
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
                    print(f"❌ {final_error}")
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
        total_files = len(json_files)
        import_logger.info(f"Trovati {total_files} file JSON da importare")
        
        # Inizializza il tracker del progresso
        progress_tracker = ProgressTracker(total_files)
        
        global table_definitions
        table_definitions = analyze_json_structure(json_files)
        
        # Usa DatabaseManager per la creazione delle tabelle
        with DatabaseManager.get_connection() as tmp_conn:
            create_dynamic_tables(tmp_conn, table_definitions)
        
        # Inizializza il pool di connessioni centralizzato
        DatabaseManager.initialize_pool(pool_size=2)  # Solo 2 connessioni per mono-processo
        
        log_resource_optimization(import_logger)
        
        # Processa file per file SEQUENZIALMENTE
        total_processed_files = 0
        total_processed_records = 0
        
        for idx, json_file in enumerate(json_files, 1):
            file_name = Path(json_file).name
            
            # Salta i file già processati con successo
            with DatabaseManager.get_pooled_connection() as conn_check:
                if is_file_processed(conn_check, file_name):
                    import_logger.info(f"⏭️ File già processato: {file_name}")
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
                            current_chunk.append((record, file_name))
                            processed_records_in_file += 1
                            
                            # Usa BATCH_SIZE dinamico invece di chunk_size fisso
                            if len(current_chunk) >= BATCH_SIZE:  # Era chunk_size, ora BATCH_SIZE (75k)
                                file_chunks.append((current_chunk, file_name, batch_id, table_definitions))
                                current_chunk = []
                        except Exception as e:
                            log_error_with_context(import_logger, e, "parsing record", file_name)
                            continue
                            
                if current_chunk:
                    file_chunks.append((current_chunk, file_name, batch_id, table_definitions))
                
                batch_logger.info(f"Chunk da processare: {len(file_chunks)} (record effettivi: {processed_records_in_file:,})")
                
                # Processa i chunk di questo file SEQUENZIALMENTE
                records_completed_in_file = 0
                if file_chunks:
                    for chunk_idx, chunk_data in enumerate(file_chunks, 1):
                        try:
                            chunk_records = len(chunk_data[0])
                            batch_logger.info(f"Processing chunk {chunk_idx}/{len(file_chunks)} ({chunk_records:,} record)")
                            
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
        import_logger.info("🎉 IMPORTAZIONE COMPLETATA!")
        log_file_progress(import_logger, final_stats['processed_files'], final_stats['total_files'], "completati")
        log_performance_stats(import_logger, "Record totali", final_stats['total_records'], total_time)
        import_logger.info(f"⏱️ Tempo totale: {str(timedelta(seconds=int(total_time)))}")
        import_logger.info("="*80)

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
        self.file_speeds = []  # Per calcolare velocità media
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
            log_performance_stats(progress_logger, "Velocità media", self.total_records_processed, elapsed_total)
    
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
                
                # ETA globale basato sui file rimanenti e velocità media
                remaining_files = self.total_files - self.processed_files
                if len(self.file_speeds) > 0 and remaining_files > 0:
                    avg_file_speed = sum(self.file_speeds[-5:]) / len(self.file_speeds[-5:])  # Media ultime 5 velocità
                    avg_records_per_file = self.total_records_processed / self.processed_files
                    estimated_remaining_records = remaining_files * avg_records_per_file
                    global_eta_seconds = estimated_remaining_records / avg_file_speed if avg_file_speed > 0 else 0
                    global_eta = str(timedelta(seconds=int(global_eta_seconds)))
                else:
                    global_eta = "N/A"
                
                progress_logger.info(f"✅ File completato: {self.current_file_name}")
                log_performance_stats(progress_logger, "Record processati", records_processed, file_time)
                log_file_progress(progress_logger, self.processed_files, self.total_files, "globale")
                log_performance_stats(progress_logger, "Record totali", self.total_records_processed, total_elapsed, "media globale")
                progress_logger.info(f"⏱️ ETA completamento: {global_eta} ({remaining_files} file rimanenti)")
                
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
    try:
        with LogContext(logger, "importazione MySQL"):
            logger.info(f"Inizio importazione: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            log_memory_status(memory_logger, "inizio")
            logger.info(f"Chunk size iniziale calcolato: {INITIAL_CHUNK_SIZE}")
            logger.info(f"Chunk size massimo calcolato: {MAX_CHUNK_SIZE}")
            
            # Verifica lo spazio disco
            check_disk_space()
            
            # Usa DatabaseManager per la connessione principale
            with DatabaseManager.get_connection() as conn:
                import_all_json_files(JSON_BASE_PATH, conn)
            
            # Chiudi il pool alla fine
            DatabaseManager.close_pool()
            logger.info("Tutte le connessioni chiuse.")
    except Exception as e:
        log_error_with_context(logger, e, "main", "importazione MySQL")
        raise

if __name__ == "__main__":
    main()
