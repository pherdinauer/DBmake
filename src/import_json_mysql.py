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
                            print(f"\n🚨 Memoria critica ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size dimezzato a {self.current_chunk_size}")
                            gc.collect()
                        elif percent_used > 0.85:  # 85% di memoria utilizzata
                            self.current_chunk_size = max(MIN_CHUNK_SIZE, int(self.current_chunk_size * 0.7))
                            print(f"\n⚠️  Memoria alta ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size ridotto a {self.current_chunk_size}")
                        elif percent_used < 0.70 and self.current_chunk_size < MAX_CHUNK_SIZE:  # 70% di memoria utilizzata
                            # Aumenta gradualmente il chunk size
                            new_size = min(MAX_CHUNK_SIZE, int(self.current_chunk_size * 1.2))
                            if new_size > self.current_chunk_size:
                                self.current_chunk_size = new_size
                                print(f"\n✅ Memoria OK ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size aumentato a {self.current_chunk_size}")
                    
                    self.last_memory_check = current_time
            except Exception as e:
                print(f"Errore nel monitoraggio memoria: {e}")
            time.sleep(0.1)  # Controlla più frequentemente

    def get_chunk_size(self):
        with self.lock:
            return self.current_chunk_size

    def stop(self):
        self.running = False
        self.monitor_thread.join()

# Funzione per connettersi a MySQL
def connect_mysql():
    max_retries = 3
    retry_delay = 5  # secondi
    
    print("\n🔍 Verifica configurazione MySQL:")
    print(f"   • Host: {MYSQL_HOST}")
    print(f"   • User: {MYSQL_USER}")
    print(f"   • Database: {MYSQL_DATABASE}")
    print(f"   • Password: {'*' * len(MYSQL_PASSWORD) if MYSQL_PASSWORD else 'non impostata'}")
    
    for attempt in range(max_retries):
        try:
            print(f"\n🔄 Tentativo di connessione {attempt + 1}/{max_retries}...")
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
            
            print("✅ Connessione riuscita!")
            return conn
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                print(f"\n⚠️ Database '{MYSQL_DATABASE}' non trovato, tentativo di creazione...")
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
                print(f"✅ Database '{MYSQL_DATABASE}' creato con successo!")
                return connect_mysql()
            elif attempt < max_retries - 1:
                print(f"\n⚠️ Tentativo di connessione {attempt + 1} fallito: {err}")
                print(f"🔄 Riprovo tra {retry_delay} secondi...")
                time.sleep(retry_delay)
            else:
                print(f"\n❌ Errore di connessione dopo {max_retries} tentativi: {err}")
                raise

# Funzione per analizzare la struttura del JSON e creare le definizioni delle tabelle
def analyze_json_structure(json_files):
    field_types = defaultdict(lambda: defaultdict(int))
    field_lengths = defaultdict(lambda: defaultdict(int))
    
    print("🔍 Analisi della struttura dei JSON...")
    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    for field, value in record.items():
                        if value is None:
                            continue
                        
                        # Normalizza i nomi dei campi
                        field = field.lower().replace(' ', '_')
                        
                        # Determina il tipo di campo
                        if isinstance(value, bool):
                            field_types[field]['BOOLEAN'] += 1
                        elif isinstance(value, int):
                            field_types[field]['INT'] += 1
                        elif isinstance(value, float):
                            field_types[field]['DOUBLE'] += 1
                        elif isinstance(value, str):
                            field_types[field]['VARCHAR'] += 1
                            field_lengths[field] = max(field_lengths[field], len(value))
                        elif isinstance(value, (list, dict)):
                            field_types[field]['JSON'] += 1
                except Exception as e:
                    print(f"⚠️ Errore nell'analisi del file {json_file}: {e}")
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
    
    return table_definitions

def create_dynamic_tables(conn, table_definitions):
    cursor = conn.cursor()
    
    # Crea la tabella principale con tutti i campi
    create_main_table = f"""
    CREATE TABLE IF NOT EXISTS main_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(f"{field} {def_type}" for field, def_type in table_definitions.items())},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file VARCHAR(255),
        batch_id VARCHAR(64),
        INDEX idx_created_at (created_at),
        INDEX idx_source_file (source_file),
        INDEX idx_batch_id (batch_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    
    cursor.execute(create_main_table)
    
    # Crea tabelle separate per i campi JSON
    for field, def_type in table_definitions.items():
        if def_type == 'JSON':
            create_json_table = f"""
            CREATE TABLE IF NOT EXISTS {field}_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                main_id INT,
                {field}_json JSON,
                source_file VARCHAR(255),
                batch_id VARCHAR(64),
                FOREIGN KEY (main_id) REFERENCES main_data(id),
                INDEX idx_main_id (main_id),
                INDEX idx_source_file (source_file),
                INDEX idx_batch_id (batch_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cursor.execute(create_processed_files)
    
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
        print(f"❌ Errore nel marcare il file come processato: {e}")
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
        
        for record, _ in batch:
            # Prepara i dati per la tabella principale
            main_values = []
            for field, def_type in table_definitions.items():
                # Normalizza il nome del campo nel record
                field_lower = field.lower().replace(' ', '_')
                value = record.get(field_lower)
                if def_type == 'JSON' and value is not None:
                    json_data[field].append((len(main_data) + 1, json.dumps(value)))
                    value = None
                main_values.append(value)
            
            # Aggiungi file_name e batch_id
            main_values.extend([file_name, batch_id])
            main_data.append(tuple(main_values))
        
        # Inserisci i dati nella tabella principale
        if main_data:
            fields = list(table_definitions.keys()) + ['source_file', 'batch_id']
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
            print("\n⚠️ Batch troppo grande, riduco la dimensione...")
            raise ValueError("BATCH_TOO_LARGE")
        raise
    except Exception as e:
        print(f"\n❌ Errore durante il processing del batch: {e}")
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
    print(f"📁 Trovati {total_files} file JSON da importare")
    
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
    
    try:
        for idx, json_file in enumerate(json_files, 1):
            file_name = os.path.basename(json_file)
            
            # Salta i file già processati con successo
            if is_file_processed(conn, file_name):
                print(f"\n⏭️  File già processato: {file_name}")
                continue
            
            file_start_time = time.time()
            file_records = 0
            batch = []
            batch_id = f"{int(time.time())}_{idx}"
            
            print("\n" + "="*80)
            print(f"📂 Processando file {idx}/{total_files}: {file_name}")
            print("="*80)
            
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
                            
                            if file_records % 1000 == 0:
                                elapsed = time.time() - file_start_time
                                speed = file_records / elapsed if elapsed > 0 else 0
                                print(f"\r📊 Record nel file: {file_records:,} | Velocità: {speed:.1f} record/s", end="")
                            
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
                                        print(f"\n🔄 Ridotto chunk size a {memory_monitor.current_chunk_size}")
                                        continue
                                    raise
                        except Exception as e:
                            print(f"\n❌ Errore nel parsing del record: {e}")
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
                            print(f"\n🔄 Ridotto chunk size a {memory_monitor.current_chunk_size}")
                            # Riprova con il batch ridotto
                            process_batch(cursor, batch, table_definitions, batch_id)
                            conn.commit()
                
                # Marca il file come processato con successo
                mark_file_processed(conn, file_name, file_records)
                
            except Exception as e:
                error_message = str(e)
                print(f"\n❌ Errore nel processing del file {file_name}: {error_message}")
                mark_file_processed(conn, file_name, file_records, 'failed', error_message)
                continue
            
            file_time = time.time() - file_start_time
            total_time_so_far += file_time
            files_processed += 1
            avg_time_per_file = total_time_so_far / files_processed
            remaining_files = total_files - files_processed
            eta_seconds = avg_time_per_file * remaining_files
            
            print("\n\n📈 Statistiche file:")
            print(f"   • Record processati: {file_records:,}")
            print(f"   • Tempo elaborazione: {str(int(file_time//60))}:{int(file_time%60):02d}")
            print(f"   • Velocità media: {file_records/file_time:.1f} record/s")
            
            print("\n📊 Statistiche totali:")
            print(f"   • Record totali: {total_records:,}")
            print(f"   • File completati: {files_processed}/{total_files}")
            print(f"   • Completamento: {(files_processed/total_files*100):.1f}%")
            print(f"   • ETA stimata: {str(int(eta_seconds//60))}:{int(eta_seconds%60):02d}")
            print(f"   • Chunk size attuale: {memory_monitor.get_chunk_size()}")
            
            gc.collect()
        
        total_time = time.time() - start_time
        print("\n" + "="*80)
        print("✨ Importazione completata!")
        print("="*80)
        print(f"📊 Statistiche finali:")
        print(f"   • Record totali: {total_records:,}")
        print(f"   • File processati: {total_files}")
        print(f"   • Chunk size finale: {memory_monitor.get_chunk_size()}")
        print(f"   • Tempo totale: {str(int(total_time//60))}:{int(total_time%60):02d}")
        print(f"   • Velocità media: {total_records/total_time:.1f} record/s")
        print("="*80)
    finally:
        memory_monitor.stop()
        cursor.close()

def main():
    print(f"🕒 Inizio importazione: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 RAM totale: {TOTAL_MEMORY_GB:.2f}GB")
    print(f"📊 RAM usabile (buffer {MEMORY_BUFFER_RATIO*100:.0f}%): {USABLE_MEMORY_GB:.2f}GB")
    print(f"📊 Chunk size iniziale calcolato: {INITIAL_CHUNK_SIZE}")
    print(f"📊 Chunk size massimo calcolato: {MAX_CHUNK_SIZE}")
    conn = connect_mysql()
    import_all_json_files(JSON_BASE_PATH, conn)
    conn.close()
    print("Tutte le connessioni chiuse.")

if __name__ == "__main__":
    main() 