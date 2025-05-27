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
BATCH_SIZE = int(os.environ.get('IMPORT_BATCH_SIZE', 50000))

# Calcola la RAM totale del sistema
TOTAL_MEMORY_BYTES = psutil.virtual_memory().total
TOTAL_MEMORY_GB = TOTAL_MEMORY_BYTES / (1024 ** 3)
MEMORY_BUFFER_RATIO = 0.3  # 30% di buffer di sicurezza per il sistema
USABLE_MEMORY_BYTES = int(TOTAL_MEMORY_BYTES * (1 - MEMORY_BUFFER_RATIO))
USABLE_MEMORY_GB = USABLE_MEMORY_BYTES / (1024 ** 3)

# Chunk size dinamico in base alla RAM
CHUNK_SIZE_INIT_RATIO = 0.01   # 1% della RAM usabile
CHUNK_SIZE_MAX_RATIO = 0.1     # 10% della RAM usabile
AVG_RECORD_SIZE_BYTES = 2 * 1024  # Stimiamo 2KB per record
INITIAL_CHUNK_SIZE = max(1000, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_INIT_RATIO) / AVG_RECORD_SIZE_BYTES))
MAX_CHUNK_SIZE = max(INITIAL_CHUNK_SIZE, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_MAX_RATIO) / AVG_RECORD_SIZE_BYTES))
MIN_CHUNK_SIZE = 100

# Limita il chunk size massimo a 10000 record per evitare problemi con max_allowed_packet
MAX_CHUNK_SIZE = min(MAX_CHUNK_SIZE, 10000)

class MemoryMonitor:
    def __init__(self, max_memory_bytes):
        self.max_memory_bytes = max_memory_bytes
        self.current_chunk_size = INITIAL_CHUNK_SIZE
        self.process = psutil.Process()
        self.lock = threading.Lock()
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_memory)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        self.last_memory_check = time.time()
        self.memory_check_interval = 0.5  # Controlla la memoria ogni 0.5 secondi

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
                            # Aumenta gradualmente il chunk size
                            new_size = min(MAX_CHUNK_SIZE, int(self.current_chunk_size * 1.2))
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

def connect_mysql():
    max_retries = 3
    retry_delay = 5  # secondi
    
    # Imposta la directory temporanea per il processo
    tmp_dir = '/database/tmp'
    os.makedirs(tmp_dir, exist_ok=True)
    os.environ['TMPDIR'] = tmp_dir
    
    logger.info("\n🔍 Verifica configurazione MySQL:")
    logger.info(f"   • Host: {MYSQL_HOST}")
    logger.info(f"   • User: {MYSQL_USER}")
    logger.info(f"   • Database: {MYSQL_DATABASE}")
    logger.info(f"   • Password: {'*' * len(MYSQL_PASSWORD) if MYSQL_PASSWORD else 'non impostata'}")
    logger.info(f"   • Directory temporanea: {tmp_dir}")
    
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
                connection_timeout=180,
                pool_size=5,
                pool_name="mypool"
            )
            
            # Imposta max_allowed_packet dopo la connessione
            cursor = conn.cursor()
            cursor.execute("SET GLOBAL max_allowed_packet=1073741824")
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
                    autocommit=True
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
        
        logger.info("\n💾 Spazio disco disponibile:")
        logger.info(f"   • /database: {database_free_gb:.1f}GB")
        logger.info(f"   • /tmp: {tmp_free_gb:.1f}GB")
        
        # Avvisa se lo spazio è basso
        if database_free_gb < 10:
            logger.warning("⚠️ Spazio disponibile su /database è basso!")
        if tmp_free_gb < 1:
            logger.warning("⚠️ Spazio disponibile su /tmp è basso!")
            
        return database_free_gb, tmp_free_gb
    except Exception as e:
        logger.error(f"❌ Errore nel controllo dello spazio disco: {e}")
        return None, None

def analyze_json_structure(json_files):
    field_types = defaultdict(lambda: defaultdict(int))
    field_lengths = defaultdict(int)
    
    logger.info("🔍 Analisi della struttura dei JSON...")
    logger.info("Questa fase analizza la struttura dei JSON per determinare:")
    logger.info("1. I tipi di dati per ogni campo")
    logger.info("2. Le lunghezze massime dei campi stringa")
    logger.info("3. I campi JSON annidati")
    
    total_files = len(json_files)
    files_analyzed = 0
    records_analyzed = 0
    start_time = time.time()
    last_progress_time = time.time()
    progress_interval = 1.0  # Aggiorna il progresso ogni secondo
    
    # Ottimizzazione: campiona solo una parte dei record per l'analisi
    SAMPLE_SIZE = 10000  # Numero di record da analizzare per file
    current_sample = 0
    
    for json_file in json_files:
        files_analyzed += 1
        file_records = 0
        file_start_time = time.time()
        current_sample = 0
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        # Ottimizzazione: analizza solo un campione dei record
                        if current_sample >= SAMPLE_SIZE:
                            break
                            
                        record = json.loads(line.strip())
                        file_records += 1
                        records_analyzed += 1
                        current_sample += 1
                        
                        # Analisi ottimizzata dei campi
                        for field, value in record.items():
                            if value is None:
                                continue
                            
                            # Normalizza i nomi dei campi
                            field = field.lower().replace(' ', '_')
                            
                            # Determina il tipo di campo (versione ottimizzata)
                            if isinstance(value, bool):
                                field_types[field]['BOOLEAN'] += 1
                            elif isinstance(value, int):
                                field_types[field]['INT'] += 1
                            elif isinstance(value, float):
                                field_types[field]['DOUBLE'] += 1
                            elif isinstance(value, str):
                                field_types[field]['VARCHAR'] += 1
                                # Ottimizzazione: aggiorna la lunghezza massima solo se necessario
                                current_length = len(value)
                                if current_length > field_lengths[field]:
                                    field_lengths[field] = current_length
                            elif isinstance(value, (list, dict)):
                                field_types[field]['JSON'] += 1
                        
                        # Aggiorna il progresso ogni secondo
                        current_time = time.time()
                        if current_time - last_progress_time >= progress_interval:
                            elapsed = current_time - file_start_time
                            speed = file_records / elapsed if elapsed > 0 else 0
                            total_elapsed = current_time - start_time
                            avg_speed = records_analyzed / total_elapsed if total_elapsed > 0 else 0
                            
                            logger.info(f"📊 Progresso: File {files_analyzed}/{total_files} | "
                                      f"Record nel file: {file_records:,} | "
                                      f"Velocità: {speed:.1f} record/s | "
                                      f"Totale: {records_analyzed:,} record ({avg_speed:.1f} record/s)")
                            
                            last_progress_time = current_time
                            
                    except Exception as e:
                        logger.error(f"⚠️ Errore nell'analisi del record nel file {json_file}: {e}")
                        continue
            
            # Log alla fine di ogni file
            file_time = time.time() - file_start_time
            total_time = time.time() - start_time
            avg_speed = records_analyzed / total_time if total_time > 0 else 0
            
            logger.info(f"✅ File {files_analyzed}/{total_files} completato:")
            logger.info(f"   • Record analizzati: {file_records:,}")
            logger.info(f"   • Tempo file: {file_time:.1f}s")
            logger.info(f"   • Velocità media: {file_records/file_time:.1f} record/s")
            logger.info(f"   • Progresso totale: {records_analyzed:,} record ({avg_speed:.1f} record/s)")
            
        except Exception as e:
            logger.error(f"⚠️ Errore nell'analisi del file {json_file}: {e}")
            continue
    
    # Crea le definizioni delle tabelle
    table_definitions = {}
    for field, types in field_types.items():
        # Determina il tipo più comune
        most_common_type = max(types.items(), key=lambda x: x[1])[0]
        
        # Crea la definizione della colonna
        if most_common_type == 'VARCHAR':
            length = min(field_lengths[field] * 2, 16383)  # Limita la lunghezza massima
            column_def = f"VARCHAR({length})"
        elif most_common_type == 'INT':
            column_def = "INT"
        elif most_common_type == 'DOUBLE':
            column_def = "DOUBLE"
        elif most_common_type == 'BOOLEAN':
            column_def = "BOOLEAN"
        elif most_common_type == 'JSON':
            column_def = "JSON"
        else:
            column_def = "TEXT"
        
        table_definitions[field] = column_def
    
    # Stampa un riepilogo della struttura trovata
    logger.info("\n📊 Struttura JSON analizzata:")
    logger.info(f"   • File analizzati: {files_analyzed}/{total_files}")
    logger.info(f"   • Record totali analizzati: {records_analyzed:,}")
    logger.info(f"   • Campi trovati: {len(table_definitions)}")
    logger.info("\nDettaglio campi:")
    for field, def_type in table_definitions.items():
        logger.info(f"   • {field}: {def_type}")
    
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
    create_main_table = f"""
    CREATE TABLE IF NOT EXISTS main_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(f"{field_mapping[field]} {column_types[field]}" for field in table_definitions.keys())},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file VARCHAR(255),
        batch_id VARCHAR(64),
        INDEX idx_created_at (created_at),
        INDEX idx_source_file (source_file),
        INDEX idx_batch_id (batch_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
    """
    
    cursor.execute(create_main_table)
    
    # Crea tabelle separate per i campi JSON
    for field, def_type in table_definitions.items():
        if def_type == 'JSON':
            sanitized_field = field_mapping[field]
            create_json_table = f"""
            CREATE TABLE IF NOT EXISTS {sanitized_field}_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                main_id INT,
                {sanitized_field}_json JSON,
                source_file VARCHAR(255),
                batch_id VARCHAR(64),
                FOREIGN KEY (main_id) REFERENCES main_data(id),
                INDEX idx_main_id (main_id),
                INDEX idx_source_file (source_file),
                INDEX idx_batch_id (batch_id)
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
        INDEX idx_status (status)
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
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sanitized_name = VALUES(sanitized_name),
                field_type = VALUES(field_type)
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
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                processed_at = CURRENT_TIMESTAMP,
                record_count = VALUES(record_count),
                status = VALUES(status),
                error_message = VALUES(error_message)
        """, (file_name, record_count, status, error_message))
        conn.commit()
    except Exception as e:
        logger.error(f"❌ Errore nel marcare il file come processato: {e}")
        conn.rollback()
    finally:
        cursor.close()

def process_batch(cursor, batch, table_definitions, batch_id):
    if not batch:
        return

    try:
        main_data = []
        json_data = defaultdict(list)
        file_name = batch[0][1]  # Prendi il nome del file dal primo record
        
        # Ottieni il mapping dei campi
        cursor.execute("SELECT original_name, sanitized_name FROM field_mapping")
        field_mapping = dict(cursor.fetchall())
        
        for record, _ in batch:
            # Prepara i dati per la tabella principale
            main_values = []
            for field, def_type in table_definitions.items():
                # Normalizza il nome del campo nel record
                field_lower = field.lower().replace(' ', '_')
                value = record.get(field_lower)
                
                # Gestisci i campi TEXT
                if def_type.startswith('VARCHAR') and len(str(value) if value else '') > 1000:
                    value = str(value)[:1000]  # Tronca a 1000 caratteri per i campi VARCHAR
                
                if def_type == 'JSON' and value is not None:
                    json_data[field_mapping[field]].append((len(main_data) + 1, json.dumps(value)))
                    value = None
                main_values.append(value)
            
            # Aggiungi file_name e batch_id
            main_values.extend([file_name, batch_id])
            main_data.append(tuple(main_values))
        
        # Inserisci i dati nella tabella principale
        if main_data:
            fields = [field_mapping[field] for field in table_definitions.keys()] + ['source_file', 'batch_id']
            placeholders = ', '.join(['%s'] * len(fields))
            insert_main = f"""
            INSERT INTO main_data ({', '.join(fields)})
            VALUES ({placeholders})
            """
            cursor.executemany(insert_main, main_data)
        
        # Inserisci i dati JSON nelle tabelle separate
        for field, data in json_data.items():
            if data:
                json_data_with_metadata = [(main_id, json_str, file_name, batch_id) for main_id, json_str in data]
                insert_json = f"""
                INSERT INTO {field}_data (main_id, {field}_json, source_file, batch_id)
                VALUES (%s, %s, %s, %s)
                """
                cursor.executemany(insert_json, json_data_with_metadata)
        
    except mysql.connector.Error as e:
        if e.errno == 1153:  # Packet too large
            logger.warning("\n⚠️ Batch troppo grande, riduco la dimensione...")
            raise ValueError("BATCH_TOO_LARGE")
        raise
    except Exception as e:
        logger.error(f"\n❌ Errore durante il processing del batch: {e}")
        raise

def find_json_files(base_path):
    json_files = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    return json_files

def import_all_json_files(base_path, conn):
    json_files = find_json_files(base_path)
    total_files = len(json_files)
    logger.info(f"📁 Trovati {total_files} file JSON da importare")
    
    # Analizza la struttura dei JSON
    table_definitions = analyze_json_structure(json_files)
    
    # Crea le tabelle dinamicamente
    create_dynamic_tables(conn, table_definitions)
    
    cursor = conn.cursor()
    memory_monitor = MemoryMonitor(USABLE_MEMORY_BYTES)
    start_time = time.time()
    total_records = 0
    files_processed = 0
    total_time_so_far = 0
    last_progress_time = time.time()
    progress_interval = 1.0  # Aggiorna il progresso ogni secondo
    
    try:
        for idx, json_file in enumerate(json_files, 1):
            file_name = os.path.basename(json_file)
            
            # Salta i file già processati con successo
            if is_file_processed(conn, file_name):
                logger.info(f"\n⏭️  File già processato: {file_name}")
                continue
            
            file_start_time = time.time()
            file_records = 0
            batch = []
            batch_id = f"{int(time.time())}_{idx}"
            
            logger.info("\n" + "="*80)
            logger.info(f"📂 Processando file {idx}/{total_files}: {file_name}")
            logger.info("="*80)
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                            batch.append((record, file_name))
                            file_records += 1
                            total_records += 1
                            
                            # Aggiorna il progresso ogni secondo
                            current_time = time.time()
                            if current_time - last_progress_time >= progress_interval:
                                elapsed = time.time() - file_start_time
                                speed = file_records / elapsed if elapsed > 0 else 0
                                total_elapsed = current_time - start_time
                                avg_speed = total_records / total_elapsed if total_elapsed > 0 else 0
                                
                                logger.info(f"📊 Progresso: File {idx}/{total_files} | "
                                          f"Record nel file: {file_records:,} | "
                                          f"Velocità: {speed:.1f} record/s | "
                                          f"Totale: {total_records:,} record ({avg_speed:.1f} record/s)")
                                
                                last_progress_time = current_time
                            
                            current_chunk_size = memory_monitor.get_chunk_size()
                            if len(batch) >= current_chunk_size:
                                try:
                                    process_batch(cursor, batch, table_definitions, batch_id)
                                    conn.commit()
                                    batch = []
                                    gc.collect()
                                except ValueError as e:
                                    if str(e) == "BATCH_TOO_LARGE":
                                        # Riduci la dimensione del batch e riprova
                                        memory_monitor.current_chunk_size = max(MIN_CHUNK_SIZE, 
                                            int(memory_monitor.current_chunk_size * 0.5))
                                        logger.warning(f"\n🔄 Ridotto chunk size a {memory_monitor.current_chunk_size}")
                                        continue
                                    raise
                        except Exception as e:
                            logger.error(f"\n❌ Errore nel parsing del record: {e}")
                            continue
                
                # Processa l'ultimo batch
                if batch:
                    try:
                        process_batch(cursor, batch, table_definitions, batch_id)
                        conn.commit()
                    except ValueError as e:
                        if str(e) == "BATCH_TOO_LARGE":
                            # Riduci la dimensione del batch e riprova
                            memory_monitor.current_chunk_size = max(MIN_CHUNK_SIZE, 
                                int(memory_monitor.current_chunk_size * 0.5))
                            logger.warning(f"\n🔄 Ridotto chunk size a {memory_monitor.current_chunk_size}")
                            # Riprova con il batch ridotto
                            process_batch(cursor, batch, table_definitions, batch_id)
                            conn.commit()
                
                # Marca il file come processato con successo
                mark_file_processed(conn, file_name, file_records)
                
            except Exception as e:
                error_message = str(e)
                logger.error(f"\n❌ Errore nel processing del file {file_name}: {error_message}")
                mark_file_processed(conn, file_name, file_records, 'failed', error_message)
                continue
            
            file_time = time.time() - file_start_time
            total_time_so_far += file_time
            files_processed += 1
            avg_time_per_file = total_time_so_far / files_processed
            remaining_files = total_files - files_processed
            eta_seconds = avg_time_per_file * remaining_files
            
            logger.info("\n\n📈 Statistiche file:")
            logger.info(f"   • Record processati: {file_records:,}")
            logger.info(f"   • Tempo elaborazione: {str(int(file_time//60))}:{int(file_time%60):02d}")
            logger.info(f"   • Velocità media: {file_records/file_time:.1f} record/s")
            
            logger.info("\n📊 Statistiche totali:")
            logger.info(f"   • Record totali: {total_records:,}")
            logger.info(f"   • File completati: {files_processed}/{total_files}")
            logger.info(f"   • Completamento: {(files_processed/total_files*100):.1f}%")
            logger.info(f"   • ETA stimata: {str(int(eta_seconds//60))}:{int(eta_seconds%60):02d}")
            logger.info(f"   • Chunk size attuale: {memory_monitor.get_chunk_size()}")
            
            gc.collect()
        
        total_time = time.time() - start_time
        logger.info("\n" + "="*80)
        logger.info("✨ Importazione completata!")
        logger.info("="*80)
        logger.info(f"📊 Statistiche finali:")
        logger.info(f"   • Record totali: {total_records:,}")
        logger.info(f"   • File processati: {total_files}")
        logger.info(f"   • Chunk size finale: {memory_monitor.get_chunk_size()}")
        logger.info(f"   • Tempo totale: {str(int(total_time//60))}:{int(total_time%60):02d}")
        logger.info(f"   • Velocità media: {total_records/total_time:.1f} record/s")
        logger.info("="*80)
    finally:
        memory_monitor.stop()
        cursor.close()

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