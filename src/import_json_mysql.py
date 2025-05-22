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

load_dotenv()

# Parametri di connessione da variabili d'ambiente
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'anac_import')
JSON_BASE_PATH = os.environ.get('ANAC_BASE_PATH', '/database/JSON')
BATCH_SIZE = int(os.environ.get('IMPORT_BATCH_SIZE', 50000))

# Calcola la RAM totale del sistema
TOTAL_MEMORY_BYTES = psutil.virtual_memory().total
TOTAL_MEMORY_GB = TOTAL_MEMORY_BYTES / (1024 ** 3)
MEMORY_BUFFER_RATIO = 0.2  # 20% di buffer di sicurezza
USABLE_MEMORY_BYTES = int(TOTAL_MEMORY_BYTES * (1 - MEMORY_BUFFER_RATIO))
USABLE_MEMORY_GB = USABLE_MEMORY_BYTES / (1024 ** 3)

# Chunk size dinamico in base alla RAM
CHUNK_SIZE_INIT_RATIO = 0.005  # 0.5% della RAM usabile
CHUNK_SIZE_MAX_RATIO = 0.05    # 5% della RAM usabile
AVG_RECORD_SIZE_BYTES = 6 * 1024
INITIAL_CHUNK_SIZE = max(1000, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_INIT_RATIO) / AVG_RECORD_SIZE_BYTES))
MAX_CHUNK_SIZE = max(INITIAL_CHUNK_SIZE, int((USABLE_MEMORY_BYTES * CHUNK_SIZE_MAX_RATIO) / AVG_RECORD_SIZE_BYTES))
MIN_CHUNK_SIZE = 100

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

    def _monitor_memory(self):
        while self.running:
            try:
                memory_info = self.process.memory_info()
                memory_usage = memory_info.rss
                percent_used = memory_usage / self.max_memory_bytes
                with self.lock:
                    if percent_used > 0.98:
                        self.current_chunk_size = max(MIN_CHUNK_SIZE, int(self.current_chunk_size * 0.5))
                        print(f"\n🚨 Memoria quasi satura ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size dimezzato a {self.current_chunk_size}")
                        gc.collect()
                    elif percent_used > 0.90:
                        self.current_chunk_size = max(MIN_CHUNK_SIZE, int(self.current_chunk_size * 0.7))
                        print(f"\n⚠️  Memoria alta ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size ridotto a {self.current_chunk_size}")
                        gc.collect()
                    elif percent_used < 0.80 and self.current_chunk_size < MAX_CHUNK_SIZE:
                        self.current_chunk_size = min(MAX_CHUNK_SIZE, int(self.current_chunk_size * 1.3))
                        print(f"\n✅ Memoria OK ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size aumentato a {self.current_chunk_size}")
            except Exception as e:
                print(f"Errore nel monitoraggio memoria: {e}")
            time.sleep(1)
    def get_chunk_size(self):
        with self.lock:
            return self.current_chunk_size
    def stop(self):
        self.running = False
        self.monitor_thread.join()

# Funzione per connettersi a MySQL
def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset='utf8mb4',
            autocommit=True
        )
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
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
            return connect_mysql()
        else:
            raise

# Funzione per creare le tabelle
CREATE_TABLES = {
    'cig': '''
        CREATE TABLE IF NOT EXISTS cig (
            cig VARCHAR(64) PRIMARY KEY,
            oggetto TEXT,
            importo DOUBLE,
            data_pubblicazione VARCHAR(32),
            data_scadenza VARCHAR(32),
            stato VARCHAR(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'bandi': '''
        CREATE TABLE IF NOT EXISTS bandi (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cig VARCHAR(64),
            tipo_bando VARCHAR(128),
            modalita_realizzazione VARCHAR(128),
            tipo_scelta_contraente VARCHAR(128),
            FOREIGN KEY (cig) REFERENCES cig(cig)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'aggiudicazioni': '''
        CREATE TABLE IF NOT EXISTS aggiudicazioni (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cig VARCHAR(64),
            importo_aggiudicazione DOUBLE,
            data_aggiudicazione VARCHAR(32),
            FOREIGN KEY (cig) REFERENCES cig(cig)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'partecipanti': '''
        CREATE TABLE IF NOT EXISTS partecipanti (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cig VARCHAR(64),
            codice_fiscale VARCHAR(32),
            ragione_sociale TEXT,
            importo_offerto DOUBLE,
            FOREIGN KEY (cig) REFERENCES cig(cig)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'varianti': '''
        CREATE TABLE IF NOT EXISTS varianti (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cig VARCHAR(64),
            importo_variante DOUBLE,
            data_variante VARCHAR(32),
            FOREIGN KEY (cig) REFERENCES cig(cig)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'raw_import': '''
        CREATE TABLE IF NOT EXISTS raw_import (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cig VARCHAR(64),
            raw_json JSON,
            source_file VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
}

def create_tables(conn):
    cursor = conn.cursor()
    for name, ddl in CREATE_TABLES.items():
        cursor.execute(ddl)
    cursor.close()

def extract_cig_data(record):
    return {
        'cig': record.get('cig', ''),
        'oggetto': record.get('oggetto', ''),
        'importo': record.get('importo', 0.0),
        'data_pubblicazione': record.get('data_pubblicazione', ''),
        'data_scadenza': record.get('data_scadenza', ''),
        'stato': record.get('stato', '')
    }

def process_record(cursor, record, source_type):
    cig = record.get('cig', '')
    if not cig:
        return
    cig_data = extract_cig_data(record)
    cursor.execute('''
        INSERT INTO cig (cig, oggetto, importo, data_pubblicazione, data_scadenza, stato)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE oggetto=VALUES(oggetto), importo=VALUES(importo), data_pubblicazione=VALUES(data_pubblicazione), data_scadenza=VALUES(data_scadenza), stato=VALUES(stato)
    ''', (
        cig_data['cig'],
        cig_data['oggetto'],
        cig_data['importo'],
        cig_data['data_pubblicazione'],
        cig_data['data_scadenza'],
        cig_data['stato']
    ))
    if source_type == 'bandi':
        cursor.execute('''
            INSERT INTO bandi (cig, tipo_bando, modalita_realizzazione, tipo_scelta_contraente)
            VALUES (%s, %s, %s, %s)
        ''', (
            cig,
            record.get('tipo_bando', ''),
            record.get('modalita_realizzazione', ''),
            record.get('tipo_scelta_contraente', '')
        ))
    elif source_type == 'aggiudicazioni':
        cursor.execute('''
            INSERT INTO aggiudicazioni (cig, importo_aggiudicazione, data_aggiudicazione)
            VALUES (%s, %s, %s)
        ''', (
            cig,
            record.get('importo_aggiudicazione', 0.0),
            record.get('data_aggiudicazione', '')
        ))
    elif source_type == 'partecipanti':
        cursor.execute('''
            INSERT INTO partecipanti (cig, codice_fiscale, ragione_sociale, importo_offerto)
            VALUES (%s, %s, %s, %s)
        ''', (
            cig,
            record.get('codice_fiscale', ''),
            record.get('ragione_sociale', ''),
            record.get('importo_offerto', 0.0)
        ))
    elif source_type == 'varianti':
        cursor.execute('''
            INSERT INTO varianti (cig, importo_variante, data_variante)
            VALUES (%s, %s, %s)
        ''', (
            cig,
            record.get('importo_variante', 0.0),
            record.get('data_variante', '')
        ))

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
    cursor = conn.cursor()
    memory_monitor = MemoryMonitor(USABLE_MEMORY_BYTES)
    start_time = time.time()
    total_records = 0
    files_processed = 0
    total_time_so_far = 0
    
    try:
        for idx, json_file in enumerate(json_files, 1):
            file_start_time = time.time()
            file_name = os.path.basename(json_file)
            source_type = file_name.split('_')[0]
            file_records = 0
            batch = []
            
            # Aggiungi una linea di separazione per ogni nuovo file
            print("\n" + "="*80)
            print(f"📂 Processando file {idx}/{total_files}: {file_name}")
            print("="*80)
            
            with open(json_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        cig = record.get('cig', None)
                        # Serializza il JSON in modo compatibile con MySQL
                        raw_json_str = json.dumps(record, ensure_ascii=True)
                        raw_json_str = raw_json_str.replace('\\', '\\\\')
                        cursor.execute(
                            "INSERT INTO raw_import (cig, raw_json, source_file) VALUES (%s, %s, %s)",
                            (cig, raw_json_str, file_name)
                        )
                        batch.append((record, file_name))
                        file_records += 1
                        total_records += 1
                        
                        # Mostra progresso ogni 1000 record
                        if file_records % 1000 == 0:
                            elapsed = time.time() - file_start_time
                            speed = file_records / elapsed if elapsed > 0 else 0
                            print(f"\r📊 Record nel file: {file_records:,} | Velocità: {speed:.1f} record/s", end="")
                        
                        current_chunk_size = memory_monitor.get_chunk_size()
                        if len(batch) >= current_chunk_size:
                            for rec, _ in batch:
                                process_record(cursor, rec, source_type)
                            conn.commit()
                            batch = []
                            gc.collect()
                    except Exception as e:
                        print(f"\n❌ Errore nel parsing o inserimento: {e}")
                        continue
            
            # Processa l'ultimo batch
            if batch:
                for rec, _ in batch:
                    process_record(cursor, rec, source_type)
                conn.commit()
            
            file_time = time.time() - file_start_time
            total_time_so_far += file_time
            files_processed += 1
            avg_time_per_file = total_time_so_far / files_processed
            remaining_files = total_files - files_processed
            eta_seconds = avg_time_per_file * remaining_files
            
            # Statistiche dettagliate dopo ogni file
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
    create_tables(conn)
    import_all_json_files(JSON_BASE_PATH, conn)
    conn.close()
    print("Tutte le connessioni chiuse.")

if __name__ == "__main__":
    main() 