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
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import multiprocessing
import csv
import tempfile
import platform

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

# Carica le variabili d'ambiente
load_dotenv()

# Parametri di connessione da variabili d'ambiente
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_USER = os.environ.get('MYSQL_USER', 'Nando')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'DataBase2025!')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'anac_import3')
JSON_BASE_PATH = os.environ.get('ANAC_BASE_PATH', '/database/JSON')
BATCH_SIZE = int(os.environ.get('IMPORT_BATCH_SIZE', 25000))  # Ridotto da 75k a 25k

# Ottimizzazione ULTRA-conservativa per stabilità MySQL
NUM_THREADS = 4  # Ridotto da 6 a 4 thread per elaborazione interna
NUM_WORKERS = 1   # MONO-PROCESSO per evitare conflitti
logger.info(f"🖥️  CPU cores disponibili: {multiprocessing.cpu_count()}")
logger.info(f"🖥️  Thread per elaborazione: {NUM_THREADS}")
logger.info(f"🖥️  Worker process: {NUM_WORKERS} (MONO-PROCESSO per stabilità)")
logger.info(f"🖥️  Batch size: {BATCH_SIZE:,} (ridotto per stabilità MySQL)")

# Directory temporanea per i file CSV - Linux optimized
TEMP_DIR_PREFERRED = '/database/tmp'
TEMP_DIR_FALLBACK = os.path.join(tempfile.gettempdir(), 'dbmake_tmp')

try:
    # Prova a creare la directory preferita
    os.makedirs(TEMP_DIR_PREFERRED, exist_ok=True)
    # Test di scrittura per verificare i permessi
    test_file = os.path.join(TEMP_DIR_PREFERRED, 'test_write.tmp')
    with open(test_file, 'w') as f:
        f.write('test')
    os.remove(test_file)
    TEMP_DIR = TEMP_DIR_PREFERRED
    logger.info(f"📁 Directory temporanea: {TEMP_DIR} ✅")
except (OSError, PermissionError) as e:
    logger.warning(f"⚠️ Impossibile usare {TEMP_DIR_PREFERRED}: {e}")
    logger.info(f"🔄 Fallback a directory temporanea di sistema...")
    os.makedirs(TEMP_DIR_FALLBACK, exist_ok=True)
    TEMP_DIR = TEMP_DIR_FALLBACK
    logger.info(f"📁 Directory temporanea: {TEMP_DIR} ✅")

# Calcola la RAM totale del sistema - aggressivo ma sicuro
TOTAL_MEMORY_BYTES = psutil.virtual_memory().total
TOTAL_MEMORY_GB = TOTAL_MEMORY_BYTES / (1024 ** 3)
MEMORY_BUFFER_RATIO = 0.2  # Solo 20% libero, 80% usabile
USABLE_MEMORY_BYTES = int(TOTAL_MEMORY_BYTES * (1 - MEMORY_BUFFER_RATIO))
USABLE_MEMORY_GB = USABLE_MEMORY_BYTES / (1024 ** 3)

# Chunk size più aggressivo con più RAM
CHUNK_SIZE_INIT_RATIO = 0.05   # 5% della RAM usabile
CHUNK_SIZE_MAX_RATIO = 0.15    # 15% della RAM usabile
AVG_RECORD_SIZE_BYTES = 2 * 1024  # Stimiamo 2KB per record
INITIAL_CHUNK_SIZE = max(1000, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_INIT_RATIO) / AVG_RECORD_SIZE_BYTES))
MAX_CHUNK_SIZE = max(INITIAL_CHUNK_SIZE, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_MAX_RATIO) / AVG_RECORD_SIZE_BYTES))
MIN_CHUNK_SIZE = 1000  # Minimo 1000 record

# Chunk size massimo aumentato per migliori performance
MAX_CHUNK_SIZE = min(MAX_CHUNK_SIZE, 75000)

class MemoryMonitor:
    def __init__(self, max_memory_bytes):
        self.max_memory_bytes = max_memory_bytes
        self.current_chunk_size = INITIAL_CHUNK_SIZE
        self.process = psutil.Process()
        self.lock = threading.Lock()
        self.running = True
        self.last_memory_check = time.time()
        self.memory_check_interval = 0.2  # Controlla la memoria più frequentemente
        self.monitor_thread = threading.Thread(target=self._monitor_memory)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def _monitor_memory(self):
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_memory_check >= self.memory_check_interval:
                    memory_info = self.process.memory_info()
                    memory_usage = memory_info.rss
                    percent_used = memory_usage / self.max_memory_bytes
                    
                    with self.lock:
                        if percent_used > 0.95:  # 95% di memoria utilizzata
                            self.current_chunk_size = max(MIN_CHUNK_SIZE, int(self.current_chunk_size * 0.5))
                            logger.warning(f"Memoria critica ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size dimezzato a {self.current_chunk_size}")
                            gc.collect()
                        elif percent_used > 0.85:  # 85% di memoria utilizzata
                            self.current_chunk_size = max(MIN_CHUNK_SIZE, int(self.current_chunk_size * 0.7))
                            logger.warning(f"Memoria alta ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size ridotto a {self.current_chunk_size}")
                        elif percent_used < 0.70 and self.current_chunk_size < MAX_CHUNK_SIZE:  # 70% di memoria utilizzata
                            # Aumenta più aggressivamente il chunk size
                            new_size = min(MAX_CHUNK_SIZE, int(self.current_chunk_size * 1.5))
                            if new_size > self.current_chunk_size:
                                self.current_chunk_size = new_size
                                logger.info(f"Memoria OK ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size aumentato a {self.current_chunk_size}")
                    
                    self.last_memory_check = current_time
            except Exception as e:
                logger.error(f"Errore nel monitoraggio memoria: {e}")
            time.sleep(0.1)

    def get_chunk_size(self):
        with self.lock:
            return self.current_chunk_size

    def stop(self):
        self.running = False
        self.monitor_thread.join()

class RecordProcessor:
    def __init__(self, table_definitions, field_mapping, file_name, batch_id):
        self.table_definitions = table_definitions
        self.field_mapping = field_mapping
        self.file_name = file_name
        self.batch_id = batch_id
        self.main_data = []
        self.json_data = defaultdict(list)
        self.lock = threading.Lock()
        self.processed_count = 0
        self.total_count = 0

    def process_record(self, record):
        main_values = []
        cig = record.get('cig', '')
        if not cig:
            return None, None

        main_values.append(cig)
        
        for field, def_type in self.table_definitions.items():
            if field.lower() == 'cig':
                continue
                
            field_lower = field.lower().replace(' ', '_')
            value = record.get(field_lower)
            
            # Handle DATE/DATETIME fields
            if def_type in ['DATE', 'DATETIME'] and value is not None:
                try:
                    # Converte vari formati di data a formato MySQL
                    if isinstance(value, str):
                        # Rimuovi eventuali caratteri extra
                        value = value.strip()
                        
                        # Se è vuoto dopo il trim, imposta a NULL
                        if not value:
                            value = None
                        # Se contiene solo 0 o valori placeholder, imposta a NULL
                        elif value in ['0', '0000-00-00', '0000-00-00 00:00:00', 'NULL', 'null']:
                            value = None
                        # Se è un timestamp unix (10-13 cifre)
                        elif value.isdigit() and len(value) in [10, 13]:
                            from datetime import datetime
                            timestamp = int(value)
                            if len(value) == 13:  # Timestamp in millisecondi
                                timestamp = timestamp / 1000
                            try:
                                dt = datetime.fromtimestamp(timestamp)
                                value = dt.strftime('%Y-%m-%d %H:%M:%S') if def_type == 'DATETIME' else dt.strftime('%Y-%m-%d')
                            except (ValueError, OSError):
                                value = None
                        # Se è un formato data riconoscibile, mantienilo
                        elif any(pattern in value for pattern in ['-', '/', ':']):
                            # Tentativi di normalizzazione
                            import re
                            # Formato ISO: YYYY-MM-DD o YYYY-MM-DD HH:MM:SS
                            if re.match(r'^\d{4}-\d{2}-\d{2}', value):
                                if def_type == 'DATE' and len(value) > 10:
                                    value = value[:10]  # Tronca la parte oraria per DATE
                            # Formato europeo: DD/MM/YYYY
                            elif re.match(r'^\d{2}/\d{2}/\d{4}', value):
                                parts = value.split('/')
                                if len(parts) >= 3:
                                    try:
                                        day, month, year = parts[0], parts[1], parts[2]
                                        value = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                                        if def_type == 'DATETIME' and len(parts) > 3:
                                            time_part = ' '.join(parts[3:])
                                            value += f" {time_part}"
                                    except:
                                        value = None
                            # Se non riconosciuto, prova a parsare con datetime
                            else:
                                try:
                                    from dateutil import parser
                                    dt = parser.parse(value)
                                    value = dt.strftime('%Y-%m-%d %H:%M:%S') if def_type == 'DATETIME' else dt.strftime('%Y-%m-%d')
                                except:
                                    value = None
                        else:
                            # Formato non riconosciuto, imposta a NULL
                            value = None
                except Exception as e:
                    logger.warning(f"⚠️ Errore nella conversione del campo data {field}: {value} - {str(e)}")
                    value = None
            
            # Handle numeric fields
            elif def_type in ['DOUBLE', 'DECIMAL'] and value is not None:
                try:
                    # Remove any non-numeric characters except decimal point and minus sign
                    if isinstance(value, str):
                        # Remove any currency symbols, spaces, and other non-numeric characters
                        value = ''.join(c for c in value if c.isdigit() or c in '.-')
                        # Handle empty string after cleaning
                        if not value:
                            value = None
                        else:
                            # Convert to float and handle potential scientific notation
                            value = float(value)
                            # Round to 2 decimal places to avoid precision issues
                            value = round(value, 2)
                except (ValueError, TypeError) as e:
                    logger.warning(f"⚠️ Errore nella conversione del campo numerico {field}: {value} - {str(e)}")
                    value = None
            
            # Handle VARCHAR fields
            elif def_type.startswith('VARCHAR'):
                if value is not None:
                    value = str(value)
                    if len(value) > 1000:
                        value = value[:1000]
                else:
                    value = None
            
            # Handle JSON fields
            elif def_type == 'JSON' and value is not None:
                with self.lock:
                    self.json_data[self.field_mapping[field]].append((cig, json.dumps(value)))
                value = None
            
            main_values.append(value)
        
        main_values.extend([self.file_name, self.batch_id])
        return tuple(main_values), None

    def add_processed(self, count):
        with self.lock:
            self.processed_count += count
            if self.processed_count % 10000 == 0:  # Log ogni 10000 record
                logger.info(f"📊 Progresso: {self.processed_count}/{self.total_count} record "
                          f"({(self.processed_count/self.total_count*100):.1f}%)")

def process_batch_parallel(batch, table_definitions, field_mapping, file_name, batch_id):
    processor = RecordProcessor(table_definitions, field_mapping, file_name, batch_id)
    processor.total_count = len(batch)
    
    # Dividi il batch in sub-batch più piccoli per ogni thread
    sub_batch_size = max(1000, len(batch) // (NUM_THREADS * 2))
    sub_batches = [batch[i:i + sub_batch_size] for i in range(0, len(batch), sub_batch_size)]
    
    # Crea una coda per i risultati
    result_queue = Queue()
    
    def worker(sub_batch):
        local_main_data = []
        local_json_data = defaultdict(list)
        
        for record, _ in sub_batch:
            main_values, _ = processor.process_record(record)
            if main_values:
                local_main_data.append(main_values)
        
        # Aggiungi i dati processati alla coda
        result_queue.put((local_main_data, local_json_data))
        processor.add_processed(len(sub_batch))
    
    # Crea e avvia i thread
    threads = []
    for sub_batch in sub_batches:
        thread = threading.Thread(target=worker, args=(sub_batch,))
        thread.start()
        threads.append(thread)
    
    # Attendi il completamento di tutti i thread
    for thread in threads:
        thread.join()
    
    # Raccogli i risultati
    while not result_queue.empty():
        local_main_data, local_json_data = result_queue.get()
        processor.main_data.extend(local_main_data)
        for field, data in local_json_data.items():
            processor.json_data[field].extend(data)
    
    return processor.main_data, processor.json_data

def write_batch_to_csv(batch_data, fields, batch_id):
    """Scrive un batch di dati in un file CSV temporaneo."""
    csv_file = os.path.join(TEMP_DIR, f'main_data_batch_{batch_id}.csv')
    
    try:
        # Debug: controlla se ci sono campi data problematici
        date_field_indices = []
        for i, field in enumerate(fields):
            if 'data_' in field.lower() or field.lower() in ['created_at', 'updated_at']:
                date_field_indices.append((i, field))
        
        if date_field_indices:
            logger.info(f"🗓️ Debug campi data nel batch {batch_id}:")
            logger.info(f"   • Campi data trovati: {[f[1] for f in date_field_indices]}")
            
            # Mostra alcuni esempi di valori data
            for row_idx, row in enumerate(batch_data[:3]):  # Solo primi 3 record
                for field_idx, field_name in date_field_indices:
                    if field_idx < len(row):
                        value = row[field_idx]
                        logger.info(f"   • Record {row_idx+1}, {field_name}: '{value}' (tipo: {type(value).__name__})")
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            # Scrivi l'header
            writer.writerow(fields)
            # Scrivi i dati
            writer.writerows(batch_data)
        
        # Verifica che il file sia stato creato
        if os.path.exists(csv_file):
            file_size = os.path.getsize(csv_file)
            logger.info(f"✅ File CSV creato: {csv_file} ({file_size} bytes)")
        else:
            logger.error(f"❌ File CSV non creato: {csv_file}")
            raise FileNotFoundError(f"File CSV non creato: {csv_file}")
            
        return csv_file
    except Exception as e:
        logger.error(f"❌ Errore nella scrittura del file CSV: {e}")
        if os.path.exists(csv_file):
            os.remove(csv_file)
        raise

def load_data_from_csv(cursor, csv_file, table_name, fields):
    """Carica i dati dal file CSV nel database usando LOAD DATA LOCAL INFILE o INSERT batch."""
    try:
        # Normalizza il path per MySQL (usa sempre forward slash)
        normalized_path = csv_file.replace('\\', '/')
        
        # Controlla la dimensione del file
        file_size = os.path.getsize(csv_file)
        file_size_mb = file_size / (1024 * 1024)
        
        logger.info(f"📥 Caricamento dati da: {csv_file}")
        logger.info(f"📥 Dimensione file: {file_size_mb:.1f}MB")
        
        # Threshold per decidere metodo di caricamento
        MAX_FILE_SIZE_MB = 50  # Se > 50MB, usa INSERT batch
        
        if file_size_mb > MAX_FILE_SIZE_MB:
            logger.warning(f"⚠️ File troppo grande ({file_size_mb:.1f}MB > {MAX_FILE_SIZE_MB}MB)")
            logger.info(f"🔄 Fallback a INSERT batch...")
            
            # Usa INSERT batch invece di LOAD DATA
            rows_affected = load_data_with_insert_batch(cursor, csv_file, table_name, fields)
            
        else:
            logger.info(f"📥 Path normalizzato: {normalized_path}")
            
            # Costruisci la query LOAD DATA
            load_data_query = f"""
            LOAD DATA LOCAL INFILE '{normalized_path}'
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            ({', '.join(fields)})
            """
            
            logger.info(f"📥 Query LOAD DATA: {load_data_query[:200]}...")
            
            # Esegui la query
            cursor.execute(load_data_query)
            
            # Verifica il numero di righe inserite
            cursor.execute("SELECT ROW_COUNT()")
            rows_affected = cursor.fetchone()[0]
        
        logger.info(f"✅ Importate {rows_affected} righe dal file {csv_file}")
        
    except mysql.connector.Error as e:
        logger.error(f"❌ Errore durante il caricamento dei dati: {e}")
        logger.error(f"❌ File: {csv_file}")
        logger.error(f"❌ Path normalizzato: {normalized_path}")
        logger.error(f"❌ File esiste: {os.path.exists(csv_file)}")
        
        # Se LOAD DATA fallisce, prova INSERT batch come fallback
        if "Got packets out of order" in str(e) or "Lost connection" in str(e):
            logger.warning(f"🔄 LOAD DATA fallito, provo INSERT batch...")
            try:
                rows_affected = load_data_with_insert_batch(cursor, csv_file, table_name, fields)
                logger.info(f"✅ Fallback riuscito: {rows_affected} righe inserite")
            except Exception as fallback_error:
                logger.error(f"❌ Anche INSERT batch fallito: {fallback_error}")
                raise
        else:
            raise
    finally:
        # Pulisci il file temporaneo
        try:
            if os.path.exists(csv_file):
                os.remove(csv_file)
                logger.info(f"🗑️ File temporaneo rimosso: {csv_file}")
        except Exception as e:
            logger.warning(f"⚠️ Impossibile rimuovere il file temporaneo {csv_file}: {e}")

def load_data_with_insert_batch(cursor, csv_file, table_name, fields):
    """Carica dati usando INSERT batch invece di LOAD DATA LOCAL INFILE."""
    import csv as csv_module
    
    logger.info(f"🔄 Caricamento con INSERT batch: {csv_file}")
    
    rows_inserted = 0
    insert_batch_size = 1000  # Insert 1000 righe alla volta
    
    # Prepara la query INSERT
    placeholders = ', '.join(['%s'] * len(fields))
    insert_query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv_module.reader(f)
            
            # Salta l'header
            next(reader)
            
            batch = []
            for row in reader:
                # Converti 'None' string a None reale per NULL MySQL
                processed_row = []
                for value in row:
                    if value == 'None' or value == '':
                        processed_row.append(None)
                    else:
                        processed_row.append(value)
                
                batch.append(processed_row)
                
                # Insert quando raggiungiamo la dimensione del batch
                if len(batch) >= insert_batch_size:
                    cursor.executemany(insert_query, batch)
                    rows_inserted += len(batch)
                    batch = []
                    
                    # Log progresso ogni 10k righe
                    if rows_inserted % 10000 == 0:
                        logger.info(f"📊 INSERT batch progresso: {rows_inserted:,} righe inserite")
            
            # Insert l'ultimo batch se non vuoto
            if batch:
                cursor.executemany(insert_query, batch)
                rows_inserted += len(batch)
        
        logger.info(f"✅ INSERT batch completato: {rows_inserted:,} righe totali")
        return rows_inserted
        
    except Exception as e:
        logger.error(f"❌ Errore in INSERT batch: {e}")
        raise

def process_batch(cursor, batch, table_definitions, batch_id):
    if not batch:
        return

    try:
        file_name = batch[0][1]
        
        # Ottieni il mapping dei campi
        try:
            cursor.execute("SELECT original_name, sanitized_name FROM field_mapping")
            field_mapping = dict(cursor.fetchall())
        except mysql.connector.Error as e:
            if e.errno == 2006:  # MySQL server has gone away
                logger.warning("⚠️ Connessione persa, riprovo...")
                raise ValueError("CONNECTION_LOST")
            raise
        
        logger.info(f"🔄 Inizio processing batch di {len(batch)} record con {NUM_THREADS} thread...")
        start_time = time.time()
        
        # Elabora il batch in parallelo
        main_data, json_data = process_batch_parallel(batch, table_definitions, field_mapping, file_name, batch_id)
        
        if main_data:
            # Prepara i campi per il CSV
            fields = ['cig'] + [field_mapping[field] for field in table_definitions.keys() if field.lower() != 'cig'] + ['source_file', 'batch_id']
            
            # Scrivi i dati in un file CSV
            csv_file = write_batch_to_csv(main_data, fields, batch_id)
            
            # Carica i dati nel database
            load_data_from_csv(cursor, csv_file, 'main_data', fields)
        
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
                        logger.warning("⚠️ Connessione persa durante l'inserimento JSON, riprovo...")
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
        speed = len(batch) / elapsed_time if elapsed_time > 0 else 0
        logger.info(f"✅ Batch completato: {len(batch)} record in {elapsed_time:.1f}s ({speed:.1f} record/s)")
        
    except mysql.connector.Error as e:
        if e.errno == 1153:  # Packet too large
            logger.warning("\n⚠️ Batch troppo grande, riduco la dimensione...")
            raise ValueError("BATCH_TOO_LARGE")
        raise
    except Exception as e:
        logger.error(f"\n❌ Errore durante il processing del batch: {e}")
        raise

def connect_mysql():
    max_retries = 3
    retry_delay = 5  # secondi
    
    # Usa la directory temporanea determinata sopra
    tmp_dir = TEMP_DIR
    
    # Pulisci la directory temporanea
    for file in os.listdir(tmp_dir):
        try:
            file_path = os.path.join(tmp_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        except:
            pass
    
    # Crea un file di configurazione MySQL temporaneo
    mysql_config = f"""[client]
tmpdir={tmp_dir}
[mysqld]
tmpdir={tmp_dir}
"""
    config_path = os.path.join(tmp_dir, 'mysql.cnf')
    with open(config_path, 'w') as f:
        f.write(mysql_config)
    
    logger.info("\n🔍 Verifica configurazione MySQL:")
    logger.info(f"   • Host: {MYSQL_HOST}")
    logger.info(f"   • User: {MYSQL_USER}")
    logger.info(f"   • Database: {MYSQL_DATABASE}")
    logger.info(f"   • Password: {'*' * len(MYSQL_PASSWORD) if MYSQL_PASSWORD else 'non impostata'}")
    logger.info(f"   • Directory temporanea: {tmp_dir}")
    logger.info(f"   • File di configurazione: {config_path}")
    
    for attempt in range(max_retries):
        try:
            logger.info(f"\n🔄 Tentativo di connessione {attempt + 1}/{max_retries}...")
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                charset='utf8mb4',
                autocommit=True,
                connect_timeout=180,
                pool_size=5,
                pool_name="mypool",
                use_pure=True,  # Usa l'implementazione Python pura
                client_flags=[mysql.connector.ClientFlag.LOCAL_FILES],  # Permetti file locali
                option_files=[config_path],  # Usa il file di configurazione
                ssl_disabled=True,  # Disabilita SSL per evitare problemi di connessione
                get_warnings=True,
                raise_on_warnings=True,
                consume_results=True,
                buffered=True,
                raw=False,
                allow_local_infile=True,
                use_unicode=True,
                auth_plugin='mysql_native_password'
            )
            
            # Imposta max_allowed_packet dopo la connessione
            cursor = conn.cursor()
            cursor.execute("SET GLOBAL max_allowed_packet=1073741824")  # 1GB
            cursor.execute("SET GLOBAL net_write_timeout=600")  # 10 minuti
            cursor.execute("SET GLOBAL net_read_timeout=600")   # 10 minuti
            cursor.execute("SET GLOBAL wait_timeout=600")       # 10 minuti
            cursor.execute("SET GLOBAL interactive_timeout=600") # 10 minuti
            cursor.close()
            
            logger.info("✅ Connessione riuscita!")
            return conn
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.warning(f"\n⚠️ Database '{MYSQL_DATABASE}' non trovato, tentativo di creazione...")
                # Crea il database se non esiste
                tmp_conn = mysql.connector.connect(
                    host=MYSQL_HOST,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    charset='utf8mb4',
                    autocommit=True,
                    option_files=[config_path],  # Usa il file di configurazione
                    ssl_disabled=True  # Disabilita SSL per evitare problemi di connessione
                )
                cursor = tmp_conn.cursor()
                cursor.execute(f"CREATE DATABASE {MYSQL_DATABASE} DEFAULT CHARACTER SET 'utf8mb4'")
                tmp_conn.close()
                logger.info(f"✅ Database '{MYSQL_DATABASE}' creato con successo!")
                return connect_mysql()
            elif attempt < max_retries - 1:
                logger.warning(f"\n⚠️ Tentativo di connessione {attempt + 1} fallito: {err}")
                logger.info(f"🔄 Riprovo tra {retry_delay} secondi...")
                time.sleep(retry_delay)
            else:
                logger.error(f"\n❌ Errore di connessione dopo {max_retries} tentativi: {err}")
                raise

def check_disk_space():
    """Verifica lo spazio disponibile su disco."""
    try:
        # Verifica lo spazio su /database
        database_stats = os.statvfs('/database')
        database_free_gb = (database_stats.f_bavail * database_stats.f_frsize) / (1024**3)
        
        # Verifica lo spazio su /tmp
        tmp_stats = os.statvfs('/tmp')
        tmp_free_gb = (tmp_stats.f_bavail * tmp_stats.f_frsize) / (1024**3)
        
        # Verifica lo spazio su /database/tmp
        tmp_dir = '/database/tmp'
        if os.path.exists(tmp_dir):
            tmp_dir_stats = os.statvfs(tmp_dir)
            tmp_dir_free_gb = (tmp_dir_stats.f_bavail * tmp_dir_stats.f_frsize) / (1024**3)
        else:
            tmp_dir_free_gb = 0
        
        logger.info("\n💾 Spazio disco disponibile:")
        logger.info(f"   • /database: {database_free_gb:.1f}GB")
        logger.info(f"   • /tmp: {tmp_free_gb:.1f}GB")
        logger.info(f"   • {tmp_dir}: {tmp_dir_free_gb:.1f}GB")
        
        # Avvisa se lo spazio è basso
        if database_free_gb < 10:
            logger.warning("⚠️ Spazio disponibile su /database è basso!")
        if tmp_free_gb < 1:
            logger.warning("⚠️ Spazio disponibile su /tmp è basso!")
        if tmp_dir_free_gb < 1:
            logger.warning(f"⚠️ Spazio disponibile su {tmp_dir} è basso!")
            
        return database_free_gb, tmp_free_gb, tmp_dir_free_gb
    except Exception as e:
        logger.error(f"❌ Errore nel controllo dello spazio disco: {e}")
        return None, None, None

def analyze_json_structure(json_files):
    field_types = defaultdict(lambda: defaultdict(int))
    field_lengths = defaultdict(int)
    field_patterns = defaultdict(set)  # Per memorizzare i pattern dei valori
    field_has_mixed = defaultdict(bool)  # Traccia se un campo ha valori misti
    
    logger.info("🔍 Analisi della struttura dei JSON...")
    logger.info("Questa fase analizza la struttura dei JSON per determinare:")
    logger.info("1. I tipi di dati per ogni campo")
    logger.info("2. Le lunghezze massime dei campi stringa")
    logger.info("3. I pattern dei valori per determinare il tipo più appropriato")
    logger.info("4. Limite: 2k righe per file + controllo memoria dinamico")
    logger.info("5. Logica CONSERVATIVA: anche un solo valore alfanumerico = VARCHAR")
    
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
    
    logger.info(f"📊 Limite righe per file: {MAX_ROWS_PER_FILE:,}")
    logger.info(f"💾 Memoria massima per analisi: {max_memory_bytes/1024/1024/1024:.1f}GB")
    
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
    
    import re
    
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
                                current_memory_gb = process.memory_info().rss / (1024**3)
                                logger.warning(f"⚠️ Limite memoria raggiunto ({current_memory_gb:.1f}GB), passo al prossimo file")
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
                            current_memory_gb = process.memory_info().rss / (1024**3)
                            
                            logger.info(f"📊 Progresso: File {files_analyzed}/{total_files} | "
                                      f"Record nel file: {file_records:,}/{MAX_ROWS_PER_FILE:,} | "
                                      f"Velocità: {speed:.1f} record/s | "
                                      f"Totale: {records_analyzed:,} record ({avg_speed:.1f} record/s) | "
                                      f"RAM: {current_memory_gb:.1f}GB")
                            
                            last_progress_time = current_time
                            
                    except Exception as e:
                        logger.error(f"⚠️ Errore nell'analisi del record nel file {json_file}: {e}")
                        continue
            
            file_time = time.time() - file_start_time
            total_time = time.time() - start_time
            avg_speed = records_analyzed / total_time if total_time > 0 else 0
            current_memory_gb = process.memory_info().rss / (1024**3)
            
            # Determina lo status del file
            if memory_exceeded:
                status = "💾 LIMITE RAM"
            elif limit_reached:
                status = "📊 LIMITE 2K"
            else:
                status = "✅"
                
            logger.info(f"{status} File {files_analyzed}/{total_files} completato:")
            logger.info(f"   • Record analizzati: {file_records:,}")
            logger.info(f"   • Tempo file: {file_time:.1f}s")
            logger.info(f"   • Velocità media: {file_records/file_time:.1f} record/s")
            logger.info(f"   • Progresso totale: {records_analyzed:,} record ({avg_speed:.1f} record/s)")
            logger.info(f"   • RAM utilizzata: {current_memory_gb:.1f}GB")
            
            # Garbage collection dopo ogni file
            gc.collect()
            
        except Exception as e:
            logger.error(f"⚠️ Errore nell'analisi del file {json_file}: {e}")
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
    final_memory_gb = process.memory_info().rss / (1024**3)
    mysql_row_limit = 65535
    
    logger.info("\n📊 Struttura JSON analizzata:")
    logger.info(f"   • File analizzati: {files_analyzed}/{total_files}")
    logger.info(f"   • Record totali analizzati: {records_analyzed:,}")
    logger.info(f"   • Media record per file: {records_analyzed/files_analyzed:.0f}")
    logger.info(f"   • Campi trovati: {len(table_definitions)}")
    logger.info(f"   • Campi misti trovati: {sum(1 for v in field_has_mixed.values() if v)}")
    logger.info(f"   • RAM finale: {final_memory_gb:.1f}GB")
    
    # Riepilogo ottimizzazioni dimensioni
    logger.info(f"\n🔧 Ottimizzazioni dimensioni tabella:")
    logger.info(f"   • Dimensione riga stimata: {total_estimated_row_size:,} bytes")
    logger.info(f"   • Limite MySQL InnoDB: {mysql_row_limit:,} bytes")
    
    if total_estimated_row_size > mysql_row_limit:
        logger.warning(f"   ⚠️  RIGA TROPPO GRANDE! Supera il limite di {(total_estimated_row_size - mysql_row_limit):,} bytes")
        logger.warning(f"   ⚠️  Convertendo più campi a TEXT...")
        
        # Riottimizza convertendo i campi più grandi a TEXT
        for field, column_def in table_definitions.items():
            if column_def.startswith('VARCHAR') and '500' in column_def:
                table_definitions[field] = 'TEXT'
                logger.info(f"       🔄 {field}: {column_def} → TEXT")
        
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
        
        logger.info(f"   ✅ Nuova dimensione riga stimata: {total_estimated_row_size:,} bytes")
    else:
        logger.info(f"   ✅ Dimensione riga OK ({(mysql_row_limit - total_estimated_row_size):,} bytes di margine)")
    
    # Mostra breakdown dei campi più grandi
    field_size_breakdown.sort(key=lambda x: x[2], reverse=True)
    logger.info(f"\n📏 Top 10 campi per dimensione:")
    for i, (field, column_def, size) in enumerate(field_size_breakdown[:10]):
        mixed_indicator = "🔀" if field_has_mixed[field] else ""
        logger.info(f"   {i+1:2d}. {field}: {column_def} ({size} bytes) {mixed_indicator}")
    
    logger.info("\nDettaglio tutti i campi:")
    for field, def_type in table_definitions.items():
        mixed_indicator = "🔀" if field_has_mixed[field] else ""
        logger.info(f"   • {field}: {def_type} {mixed_indicator}")
        if field in field_patterns:
            logger.info(f"     Pattern trovati: {', '.join(sorted(field_patterns[field]))}")
    
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
    cursor = conn.cursor()
    try:
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        
        logger.info(f"\n🔍 Struttura tabella {table_name}:")
        date_columns = []
        for column in columns:
            field_name, field_type, is_null, key, default, extra = column
            logger.info(f"   • {field_name}: {field_type}")
            
            # Evidenzia i campi data
            if 'data_' in field_name.lower() or field_name.lower() in ['created_at', 'updated_at']:
                date_columns.append((field_name, field_type))
        
        if date_columns:
            logger.info(f"\n🗓️ Campi data nella tabella:")
            for field_name, field_type in date_columns:
                status = "✅" if field_type.upper() in ['DATE', 'DATETIME', 'TIMESTAMP'] else "❌"
                logger.info(f"   {status} {field_name}: {field_type}")
                
    except mysql.connector.Error as e:
        logger.error(f"❌ Errore nella verifica struttura tabella: {e}")
    finally:
        cursor.close()

def create_dynamic_tables(conn, table_definitions):
    cursor = conn.cursor()
    
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
    
    # Crea la tabella principale con tutti i campi
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
    
    logger.info(f"🏗️ Creazione tabella main_data...")
    logger.info(f"🏗️ Campi principali: {len(main_fields)}")
    
    cursor.execute(create_main_table)
    
    # Verifica la struttura creata
    verify_table_structure(conn)
    
    # Crea tabelle separate per i campi JSON
    for field, def_type in table_definitions.items():
        if def_type == 'JSON':
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
    
    # Inserisci il mapping dei campi
    for field, def_type in table_definitions.items():
        cursor.execute("""
            INSERT INTO field_mapping (original_name, sanitized_name, field_type)
            VALUES (%s, %s, %s) AS new_data
            ON DUPLICATE KEY UPDATE
                sanitized_name = new_data.sanitized_name,
                field_type = new_data.field_type
        """, (field, field_mapping[field], column_types[field]))
    
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
            VALUES (%s, %s, %s, %s) AS new_data
            ON DUPLICATE KEY UPDATE
                processed_at = CURRENT_TIMESTAMP,
                record_count = new_data.record_count,
                status = new_data.status,
                error_message = new_data.error_message
        """, (file_name, record_count, status, error_message))
        conn.commit()
    except Exception as e:
        logger.error(f"❌ Errore nel marcare il file come processato: {e}")
        conn.rollback()
    finally:
        cursor.close()

def find_json_files(base_path):
    json_files = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    return json_files

def process_chunk(args):
    chunk, file_name, batch_id, table_definitions = args
    print(f"[Multiprocessing] Processo PID={os.getpid()} elabora chunk di {len(chunk)} record del file {file_name}")
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            conn = connection_pool.get_connection()
            cursor = conn.cursor()
            try:
                process_batch(cursor, chunk, table_definitions, batch_id)
                conn.commit()
                return  # Successo
            except Exception as e:
                logger.error(f"Errore nel processare chunk (tentativo {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Backoff exponenziale
                else:
                    raise
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Errore nel processare chunk dopo {max_retries} tentativi: {e}")
                raise

def test_mysql_file_access(conn):
    """Testa se MySQL può accedere ai file nella directory temporanea."""
    cursor = conn.cursor()
    try:
        # Crea un file di test
        test_file = os.path.join(TEMP_DIR, 'mysql_access_test.csv')
        with open(test_file, 'w') as f:
            f.write('test_col\n"test_value"\n')
        
        # Verifica che il file esista
        if not os.path.exists(test_file):
            logger.error(f"❌ File di test non creato: {test_file}")
            return False
        
        logger.info(f"🧪 Test accesso MySQL al file: {test_file}")
        
        # Crea una tabella temporanea per il test
        cursor.execute("""
            CREATE TEMPORARY TABLE test_load (
                test_col VARCHAR(50)
            )
        """)
        
        # Prova LOAD DATA LOCAL INFILE
        load_query = f"""
            LOAD DATA LOCAL INFILE '{test_file}'
            INTO TABLE test_load
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
        """
        
        cursor.execute(load_query)
        
        # Verifica che i dati siano stati caricati
        cursor.execute("SELECT COUNT(*) FROM test_load")
        count = cursor.fetchone()[0]
        
        if count > 0:
            logger.info(f"✅ Test MySQL file access: SUCCESS ({count} righe caricate)")
            success = True
        else:
            logger.warning(f"⚠️ Test MySQL file access: Nessuna riga caricata")
            success = False
        
        # Pulisci
        cursor.execute("DROP TEMPORARY TABLE test_load")
        os.remove(test_file)
        
        return success
        
    except mysql.connector.Error as e:
        logger.error(f"❌ Test MySQL file access FAILED: {e}")
        # Pulisci in caso di errore
        try:
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS test_load")
            if os.path.exists(test_file):
                os.remove(test_file)
        except:
            pass
        return False
    finally:
        cursor.close()

def import_all_json_files(base_path, conn):
    json_files = find_json_files(base_path)
    total_files = len(json_files)
    logger.info(f"📁 Trovati {total_files} file JSON da importare")
    
    # Test accesso file MySQL
    if not test_mysql_file_access(conn):
        logger.error("❌ MySQL non può accedere ai file temporanei!")
        logger.error("❌ Verifica configurazione secure_file_priv e local_infile")
        return
    
    global table_definitions
    table_definitions = analyze_json_structure(json_files)
    # Usa una connessione temporanea per la creazione delle tabelle
    tmp_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset='utf8mb4',
        autocommit=True
    )
    create_dynamic_tables(tmp_conn, table_definitions)
    tmp_conn.close()
    # Pool di connessioni ridotto per stabilità (MONO-PROCESSO)
    global connection_pool
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="mypool",
        pool_size=2,  # Solo 2 connessioni per mono-processo
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset='utf8mb4',
        autocommit=True,
        connect_timeout=180,
        use_pure=True,
        client_flags=[mysql.connector.ClientFlag.LOCAL_FILES],
        ssl_disabled=True,
        get_warnings=True,
        raise_on_warnings=True,
        consume_results=True,
        buffered=True,
        raw=False,
        allow_local_infile=True,
        use_unicode=True,
        auth_plugin='mysql_native_password'
    )
    
    logger.info("📊 Configurazione risorse MONO-PROCESSO:")
    logger.info(f"   • RAM totale: {TOTAL_MEMORY_GB:.1f}GB")
    logger.info(f"   • RAM usabile: {USABLE_MEMORY_GB:.1f}GB (buffer {MEMORY_BUFFER_RATIO*100:.0f}%)")
    logger.info(f"   • Worker process: {NUM_WORKERS}/8 core (MONO-PROCESSO)")
    logger.info(f"   • Batch size: {BATCH_SIZE:,}")
    logger.info(f"   • Pool connessioni: 2 (MINIMAL)")
    logger.info(f"   • Chunk size max: {MAX_CHUNK_SIZE:,}")
    
    # Processa file per file SEQUENZIALMENTE
    total_processed_files = 0
    total_processed_records = 0
    
    for idx, json_file in enumerate(json_files, 1):
        file_name = os.path.basename(json_file)
        
        # Salta i file già processati con successo
        conn_check = connection_pool.get_connection()
        if is_file_processed(conn_check, file_name):
            logger.info(f"\n⏭️  File già processato: {file_name}")
            conn_check.close()
            continue
        conn_check.close()
        
        logger.info(f"\n📂 Processando file {idx}/{total_files}: {file_name}")
        file_start_time = time.time()
        
        batch_id = f"{int(time.time())}_{idx}"
        chunk_size = BATCH_SIZE
        file_chunks = []
        current_chunk = []
        file_record_count = 0
        
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
                        file_record_count += 1
                        
                        if len(current_chunk) >= chunk_size:
                            file_chunks.append((current_chunk, file_name, batch_id, table_definitions))
                            current_chunk = []
                    except Exception as e:
                        logger.error(f"\n❌ Errore nel parsing del record: {e}")
                        continue
                        
                if current_chunk:
                    file_chunks.append((current_chunk, file_name, batch_id, table_definitions))
            
            logger.info(f"🚀 File: {file_name} - Chunk da processare: {len(file_chunks)} (record: {file_record_count:,})")
            
            # Processa i chunk di questo file SEQUENZIALMENTE (no multiprocessing)
            if file_chunks:
                for chunk_data in file_chunks:
                    try:
                        process_chunk_sequential(chunk_data)
                    except Exception as e:
                        logger.error(f"❌ Errore nel processing chunk: {e}")
                        # Continua con il prossimo chunk invece di fallire tutto
                        continue
                
                # Marca il file come processato
                conn_mark = connection_pool.get_connection()
                mark_file_processed(conn_mark, file_name, file_record_count)
                conn_mark.close()
                
                total_processed_files += 1
                total_processed_records += file_record_count
                
                file_time = time.time() - file_start_time
                logger.info(f"✅ File completato: {file_name}")
                logger.info(f"   • Record processati: {file_record_count:,}")
                logger.info(f"   • Tempo elaborazione: {file_time:.1f}s")
                logger.info(f"   • Velocità: {file_record_count/file_time:.1f} record/s")
                logger.info(f"   • Progresso: {total_processed_files}/{total_files} file ({total_processed_files/total_files*100:.1f}%)")
            
            # Libera la memoria dei chunk di questo file
            del file_chunks
            del current_chunk
            gc.collect()
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"\n❌ Errore nel processing del file {file_name}: {error_message}")
            conn_mark = connection_pool.get_connection()
            mark_file_processed(conn_mark, file_name, file_record_count, 'failed', error_message)
            conn_mark.close()
    
    logger.info("\n" + "="*80)
    logger.info("✨ Importazione completata!")
    logger.info(f"📊 File processati: {total_processed_files}/{total_files}")
    logger.info(f"📊 Record totali: {total_processed_records:,}")
    logger.info("="*80)

def process_chunk_sequential(args):
    """Processa un chunk in modo sequenziale senza multiprocessing."""
    chunk, file_name, batch_id, table_definitions = args
    logger.info(f"[Sequential] Elabora chunk di {len(chunk)} record del file {file_name}")
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            conn = connection_pool.get_connection()
            cursor = conn.cursor()
            try:
                process_batch(cursor, chunk, table_definitions, batch_id)
                conn.commit()
                return  # Successo
            except Exception as e:
                logger.error(f"Errore nel processare chunk (tentativo {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Backoff exponenziale
                else:
                    raise
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Errore nel processare chunk dopo {max_retries} tentativi: {e}")
                raise

def main():
    try:
        # Crea la directory dei log se non esiste
        os.makedirs('logs', exist_ok=True)
        
        logger.info(f"🕒 Inizio importazione: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 RAM totale: {TOTAL_MEMORY_GB:.2f}GB")
        logger.info(f"📊 RAM usabile (buffer {MEMORY_BUFFER_RATIO*100:.0f}%): {USABLE_MEMORY_GB:.2f}GB")
        logger.info(f"📊 Chunk size iniziale calcolato: {INITIAL_CHUNK_SIZE}")
        logger.info(f"📊 Chunk size massimo calcolato: {MAX_CHUNK_SIZE}")
        
        # Verifica lo spazio disco
        check_disk_space()
        
        conn = connect_mysql()
        import_all_json_files(JSON_BASE_PATH, conn)
        conn.close()
        logger.info("Tutte le connessioni chiuse.")
    except Exception as e:
        logger.error(f"Errore durante l'importazione in MySQL: {e}")
        raise

if __name__ == "__main__":
    main() 