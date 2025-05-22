import os
import json
import glob
import argparse
import time
from datetime import datetime, timedelta
import gc  # Garbage Collector
import psutil
import threading
import math

# Configurazione
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'anac_import')
JSON_BASE_PATH = os.environ.get('ANAC_BASE_PATH', '/database/JSON')
SQL_FILE = os.environ.get('MYSQL_SQL_FILE', 'export_mysql.sql')

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

# Configurazione memoria
# Calcola la RAM totale del sistema
TOTAL_MEMORY_BYTES = psutil.virtual_memory().total
TOTAL_MEMORY_GB = TOTAL_MEMORY_BYTES / (1024 ** 3)
MEMORY_BUFFER_RATIO = 0.2  # 20% di buffer di sicurezza
USABLE_MEMORY_BYTES = int(TOTAL_MEMORY_BYTES * (1 - MEMORY_BUFFER_RATIO))
USABLE_MEMORY_GB = USABLE_MEMORY_BYTES / (1024 ** 3)

# Chunk size dinamico in base alla RAM
CHUNK_SIZE_INIT_RATIO = 0.005  # 0.5% della RAM usabile
CHUNK_SIZE_MAX_RATIO = 0.05    # 5% della RAM usabile

# Stima: ogni record medio pesa 6KB (puoi regolare se vuoi più aggressivo)
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
                memory_usage = memory_info.rss  # Resident Set Size
                percent_used = memory_usage / self.max_memory_bytes

                with self.lock:
                    if percent_used > 0.98:  # Sopra il 98% del limite
                        self.current_chunk_size = max(MIN_CHUNK_SIZE, int(self.current_chunk_size * 0.5))
                        print(f"\n🚨 Memoria quasi satura ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size dimezzato a {self.current_chunk_size}")
                        gc.collect()
                    elif percent_used > 0.90:  # Sopra il 90% del limite
                        self.current_chunk_size = max(MIN_CHUNK_SIZE, int(self.current_chunk_size * 0.7))
                        print(f"\n⚠️  Memoria alta ({memory_usage/1024/1024/1024:.1f}GB/{USABLE_MEMORY_GB:.1f}GB), chunk size ridotto a {self.current_chunk_size}")
                        gc.collect()
                    elif percent_used < 0.80 and self.current_chunk_size < MAX_CHUNK_SIZE:  # Sotto l'80% del limite
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

def find_json_files(base_path):
    json_files = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    return json_files

def sql_escape(val):
    if val is None:
        return 'NULL'
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"

def format_time(seconds):
    return str(timedelta(seconds=int(seconds)))

def write_insert_chunk(f, table, columns, rows):
    if not rows:
        return
    f.write(f"INSERT INTO {table} ({columns}) VALUES\n  " + ',\n  '.join(rows) + ";\n\n")
    f.flush()  # Forza la scrittura su disco
    os.fsync(f.fileno())  # Assicura che i dati siano scritti su disco

def process_record(record, file_name, source_type, f, rows_buffer):
    cig = record.get('cig', '')
    
    # Serializza il JSON in modo compatibile con MySQL
    raw_json_str = json.dumps(record, ensure_ascii=True)
    raw_json_str = raw_json_str.replace('\\', '\\\\')  # Raddoppia i backslash
    raw_import_row = f"(NULL, {sql_escape(cig)}, {sql_escape(raw_json_str)}, {sql_escape(file_name)})"
    rows_buffer['raw_import'].append(raw_import_row)
    
    if cig:
        if source_type == 'bandi':
            bandi_row = f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('tipo_bando', ''))}, {sql_escape(record.get('modalita_realizzazione', ''))}, {sql_escape(record.get('tipo_scelta_contraente', ''))})"
            rows_buffer['bandi'].append(bandi_row)
        elif source_type == 'aggiudicazioni':
            aggiudicazioni_row = f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('importo_aggiudicazione', 0.0))}, {sql_escape(record.get('data_aggiudicazione', ''))})"
            rows_buffer['aggiudicazioni'].append(aggiudicazioni_row)
        elif source_type == 'partecipanti':
            partecipanti_row = f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('codice_fiscale', ''))}, {sql_escape(record.get('ragione_sociale', ''))}, {sql_escape(record.get('importo_offerto', 0.0))})"
            rows_buffer['partecipanti'].append(partecipanti_row)
        elif source_type == 'varianti':
            varianti_row = f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('importo_variante', 0.0))}, {sql_escape(record.get('data_variante', ''))})"
            rows_buffer['varianti'].append(varianti_row)
        
        if source_type in ['bandi', 'aggiudicazioni', 'partecipanti', 'varianti']:
            cig_row = f"({sql_escape(cig)}, {sql_escape(record.get('oggetto', ''))}, {sql_escape(record.get('importo', 0.0))}, {sql_escape(record.get('data_pubblicazione', ''))}, {sql_escape(record.get('data_scadenza', ''))}, {sql_escape(record.get('stato', ''))}, NOW())"
            rows_buffer['cig'].append(cig_row)

def flush_buffers(f, rows_buffer):
    for table, rows in rows_buffer.items():
        if rows:
            write_insert_chunk(f, table, get_columns_for_table(table), rows)
            rows_buffer[table] = []

def get_columns_for_table(table):
    columns = {
        'cig': 'cig, oggetto, importo, data_pubblicazione, data_scadenza, stato, created_at',
        'bandi': 'id, cig, tipo_bando, modalita_realizzazione, tipo_scelta_contraente',
        'aggiudicazioni': 'id, cig, importo_aggiudicazione, data_aggiudicazione',
        'partecipanti': 'id, cig, codice_fiscale, ragione_sociale, importo_offerto',
        'varianti': 'id, cig, importo_variante, data_variante',
        'raw_import': 'id, cig, raw_json, source_file'
    }
    return columns.get(table, '')

def main():
    parser = argparse.ArgumentParser(description='Esporta dati JSON in formato SQL per MySQL')
    parser.add_argument('--chunk-size', type=int, default=INITIAL_CHUNK_SIZE,
                      help=f'Chunk size iniziale (default: {INITIAL_CHUNK_SIZE})')
    args = parser.parse_args()
    
    # Inizializza il monitor di memoria
    memory_monitor = MemoryMonitor(USABLE_MEMORY_BYTES)
    
    start_time = time.time()
    print(f"🕒 Inizio esportazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 RAM totale: {TOTAL_MEMORY_GB:.2f}GB")
    print(f"📊 RAM usabile (buffer {MEMORY_BUFFER_RATIO*100:.0f}%): {USABLE_MEMORY_GB:.2f}GB")
    print(f"📊 Chunk size iniziale calcolato: {INITIAL_CHUNK_SIZE}")
    print(f"📊 Chunk size massimo calcolato: {MAX_CHUNK_SIZE}")

    try:
        with open(SQL_FILE, 'w', encoding='utf-8', buffering=1) as f:
            # CREATE DATABASE
            f.write(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` DEFAULT CHARACTER SET utf8mb4;\nUSE `{MYSQL_DATABASE}`;\n\n")
            # CREATE TABLES
            for ddl in CREATE_TABLES.values():
                f.write(ddl + '\n')
            f.write('\n')
            f.flush()

            json_files = find_json_files(JSON_BASE_PATH)
            total_files = len(json_files)
            print(f"📁 Trovati {total_files} file JSON da esportare")
            
            total_records = 0
            files_processed = 0
            total_time_so_far = 0
            
            for idx, json_file in enumerate(json_files, 1):
                file_start_time = time.time()
                file_name = os.path.basename(json_file)
                source_type = file_name.split('_')[0]
                
                file_records = 0
                rows_buffer = {
                    'cig': [], 'bandi': [], 'aggiudicazioni': [],
                    'partecipanti': [], 'varianti': [], 'raw_import': []
                }
                
                with open(json_file, 'r', encoding='utf-8') as jf:
                    for line in jf:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                            file_records += 1
                            total_records += 1
                            
                            # Processa il record
                            process_record(record, file_name, source_type, f, rows_buffer)
                            
                            # Controlla se dobbiamo scrivere i buffer
                            current_chunk_size = memory_monitor.get_chunk_size()
                            if any(len(rows) >= current_chunk_size for rows in rows_buffer.values()):
                                flush_buffers(f, rows_buffer)
                                gc.collect()
                            
                        except Exception as e:
                            print(f"❌ Errore nel parsing o esportazione: {e}")
                            continue
                
                # Scrivi eventuali record rimasti nel buffer
                flush_buffers(f, rows_buffer)
                
                file_time = time.time() - file_start_time
                total_time_so_far += file_time
                files_processed += 1
                
                # Calcola statistiche e ETA
                avg_time_per_file = total_time_so_far / files_processed
                remaining_files = total_files - files_processed
                eta_seconds = avg_time_per_file * remaining_files
                
                # Mostra progresso alla fine di ogni file
                print(f"\n📊 Progresso file {idx}/{total_files}:")
                print(f"   • File: {file_name}")
                print(f"   • Record processati: {file_records:,}")
                print(f"   • Tempo file: {format_time(file_time)}")
                print(f"   • Record totali: {total_records:,}")
                print(f"   • Velocità media: {total_records/total_time_so_far:.1f} record/s")
                print(f"   • ETA totale: {format_time(eta_seconds)}")
                print(f"   • Completamento: {(files_processed/total_files*100):.1f}%")
                print(f"   • Chunk size attuale: {memory_monitor.get_chunk_size()}")
                
                # Forza garbage collection dopo ogni file
                gc.collect()
        
        total_time = time.time() - start_time
        print(f"\n✨ Esportazione completata!")
        print(f"📊 Statistiche finali:")
        print(f"   • Record totali: {total_records:,}")
        print(f"   • File processati: {total_files}")
        print(f"   • Chunk size finale: {memory_monitor.get_chunk_size()}")
        print(f"   • Tempo totale: {format_time(total_time)}")
        print(f"   • Velocità media: {total_records/total_time:.1f} record/s")
        print(f"   • File generato: {SQL_FILE}")
        print(f"🕒 Fine esportazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    finally:
        memory_monitor.stop()

if __name__ == "__main__":
    main() 