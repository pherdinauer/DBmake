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
                            gc.collect()
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
            oggetto_gara TEXT,
            oggetto_lotto TEXT,
            oggetto_principale_contratto VARCHAR(128),
            importo_lotto DOUBLE,
            importo_complessivo_gara DOUBLE,
            stato VARCHAR(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_stato (stato),
            INDEX idx_importo (importo_complessivo_gara)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'localizzazione': '''
        CREATE TABLE IF NOT EXISTS localizzazione (
            cig VARCHAR(64) PRIMARY KEY,
            citta VARCHAR(128),
            regione VARCHAR(128),
            indirizzo TEXT,
            istat_comune VARCHAR(32),
            sezione_regionale VARCHAR(128),
            FOREIGN KEY (cig) REFERENCES cig(cig),
            INDEX idx_citta (citta),
            INDEX idx_regione (regione)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'temporale': '''
        CREATE TABLE IF NOT EXISTS temporale (
            cig VARCHAR(64) PRIMARY KEY,
            data_comunicazione DATE,
            anno_comunicazione INT,
            mese_comunicazione VARCHAR(2),
            FOREIGN KEY (cig) REFERENCES cig(cig),
            INDEX idx_data (data_comunicazione),
            INDEX idx_anno (anno_comunicazione)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'amministrazione': '''
        CREATE TABLE IF NOT EXISTS amministrazione (
            cig VARCHAR(64) PRIMARY KEY,
            cf_amministrazione_appaltante VARCHAR(32),
            denominazione_amministrazione_appaltante TEXT,
            id_centro_costo VARCHAR(64),
            denominazione_centro_costo TEXT,
            FOREIGN KEY (cig) REFERENCES cig(cig),
            INDEX idx_cf (cf_amministrazione_appaltante)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'contratto': '''
        CREATE TABLE IF NOT EXISTS contratto (
            cig VARCHAR(64) PRIMARY KEY,
            tipo_scelta_contraente VARCHAR(128),
            cod_tipo_scelta_contraente VARCHAR(32),
            tipo_fattispecie_contrattuale VARCHAR(128),
            id_tipo_fattispecie_contrattuale VARCHAR(32),
            n_lotti_componenti VARCHAR(32),
            FOREIGN KEY (cig) REFERENCES cig(cig),
            INDEX idx_tipo_scelta (tipo_scelta_contraente)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    ''',
    'raw_import': '''
        CREATE TABLE IF NOT EXISTS raw_import (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cig VARCHAR(64),
            raw_json JSON,
            source_file VARCHAR(255),
            INDEX idx_cig (cig)
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

def process_batch(cursor, batch, source_type):
    if not batch:
        return

    try:
        # Prepara i dati per l'inserimento bulk
        cig_data = []
        localizzazione_data = []
        temporale_data = []
        amministrazione_data = []
        contratto_data = []
        raw_import_data = []

        for record, file_name in batch:
            cig = record.get('cig', '')
            if not cig:
                continue

            # Prepara i dati per raw_import
            raw_json_str = json.dumps(record, ensure_ascii=True).replace('\\', '\\\\')
            raw_import_data.append((cig, raw_json_str, file_name))

            # Prepara i dati per cig
            cig_data.append((
                cig,
                record.get('oggetto_gara', ''),
                record.get('oggetto_lotto', ''),
                record.get('oggetto_principale_contratto', ''),
                record.get('importo_lotto', 0.0),
                record.get('importo_complessivo_gara', 0.0),
                record.get('stato', '')
            ))

            # Prepara i dati per localizzazione
            localizzazione_data.append((
                cig,
                record.get('citta', ''),
                record.get('regione', ''),
                record.get('indirizzo', ''),
                record.get('istat_comune', ''),
                record.get('sezione_regionale', '')
            ))

            # Prepara i dati per temporale
            temporale_data.append((
                cig,
                record.get('data_comunicazione', None),
                record.get('anno_comunicazione', None),
                record.get('mese_comunicazione', '')
            ))

            # Prepara i dati per amministrazione
            amministrazione_data.append((
                cig,
                record.get('cf_amministrazione_appaltante', ''),
                record.get('denominazione_amministrazione_appaltante', ''),
                record.get('id_centro_costo', ''),
                record.get('denominazione_centro_costo', '')
            ))

            # Prepara i dati per contratto
            contratto_data.append((
                cig,
                record.get('tipo_scelta_contraente', ''),
                record.get('cod_tipo_scelta_contraente', ''),
                record.get('tipo_fattispecie_contrattuale', ''),
                record.get('id_tipo_fattispecie_contrattuale', ''),
                record.get('n_lotti_componenti', '')
            ))

        # Esegui gli insert bulk
        if raw_import_data:
            cursor.executemany(
                "INSERT INTO raw_import (cig, raw_json, source_file) VALUES (%s, %s, %s)",
                raw_import_data
            )

        if cig_data:
            cursor.executemany('''
                INSERT INTO cig (cig, oggetto_gara, oggetto_lotto, oggetto_principale_contratto, 
                               importo_lotto, importo_complessivo_gara, stato)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    oggetto_gara=VALUES(oggetto_gara),
                    oggetto_lotto=VALUES(oggetto_lotto),
                    oggetto_principale_contratto=VALUES(oggetto_principale_contratto),
                    importo_lotto=VALUES(importo_lotto),
                    importo_complessivo_gara=VALUES(importo_complessivo_gara),
                    stato=VALUES(stato)
            ''', cig_data)

        if localizzazione_data:
            cursor.executemany('''
                INSERT INTO localizzazione (cig, citta, regione, indirizzo, istat_comune, sezione_regionale)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    citta=VALUES(citta),
                    regione=VALUES(regione),
                    indirizzo=VALUES(indirizzo),
                    istat_comune=VALUES(istat_comune),
                    sezione_regionale=VALUES(sezione_regionale)
            ''', localizzazione_data)

        if temporale_data:
            cursor.executemany('''
                INSERT INTO temporale (cig, data_comunicazione, anno_comunicazione, mese_comunicazione)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    data_comunicazione=VALUES(data_comunicazione),
                    anno_comunicazione=VALUES(anno_comunicazione),
                    mese_comunicazione=VALUES(mese_comunicazione)
            ''', temporale_data)

        if amministrazione_data:
            cursor.executemany('''
                INSERT INTO amministrazione (cig, cf_amministrazione_appaltante, 
                                           denominazione_amministrazione_appaltante,
                                           id_centro_costo, denominazione_centro_costo)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    cf_amministrazione_appaltante=VALUES(cf_amministrazione_appaltante),
                    denominazione_amministrazione_appaltante=VALUES(denominazione_amministrazione_appaltante),
                    id_centro_costo=VALUES(id_centro_costo),
                    denominazione_centro_costo=VALUES(denominazione_centro_costo)
            ''', amministrazione_data)

        if contratto_data:
            cursor.executemany('''
                INSERT INTO contratto (cig, tipo_scelta_contraente, cod_tipo_scelta_contraente,
                                      tipo_fattispecie_contrattuale, id_tipo_fattispecie_contrattuale,
                                      n_lotti_componenti)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    tipo_scelta_contraente=VALUES(tipo_scelta_contraente),
                    cod_tipo_scelta_contraente=VALUES(cod_tipo_scelta_contraente),
                    tipo_fattispecie_contrattuale=VALUES(tipo_fattispecie_contrattuale),
                    id_tipo_fattispecie_contrattuale=VALUES(id_tipo_fattispecie_contrattuale),
                    n_lotti_componenti=VALUES(n_lotti_componenti)
            ''', contratto_data)

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
                            process_batch(cursor, batch, source_type)
                            conn.commit()
                            batch = []
                            gc.collect()
                    except Exception as e:
                        print(f"\n❌ Errore nel parsing o inserimento: {e}")
                        continue
            
            # Processa l'ultimo batch
            if batch:
                process_batch(cursor, batch, source_type)
                conn.commit()
            
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
    create_tables(conn)
    import_all_json_files(JSON_BASE_PATH, conn)
    conn.close()
    print("Tutte le connessioni chiuse.")

if __name__ == "__main__":
    main() 