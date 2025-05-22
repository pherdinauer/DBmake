import os
import json
import glob
import argparse
import time
from datetime import datetime, timedelta
import gc  # Garbage Collector

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

# Buffer più piccolo per default
DEFAULT_CHUNK_SIZE = 1000

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

def process_record(record, file_name, source_type, f, chunk_size):
    cig = record.get('cig', '')
    
    # Processa raw_import
    raw_import_row = f"(NULL, {sql_escape(cig)}, {sql_escape(json.dumps(record, ensure_ascii=False))}, {sql_escape(file_name)})"
    write_insert_chunk(f, 'raw_import', 'id, cig, raw_json, source_file', [raw_import_row])
    
    # Processa dati relazionali
    if cig:
        if source_type == 'bandi':
            bandi_row = f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('tipo_bando', ''))}, {sql_escape(record.get('modalita_realizzazione', ''))}, {sql_escape(record.get('tipo_scelta_contraente', ''))})"
            write_insert_chunk(f, 'bandi', 'id, cig, tipo_bando, modalita_realizzazione, tipo_scelta_contraente', [bandi_row])
        elif source_type == 'aggiudicazioni':
            aggiudicazioni_row = f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('importo_aggiudicazione', 0.0))}, {sql_escape(record.get('data_aggiudicazione', ''))})"
            write_insert_chunk(f, 'aggiudicazioni', 'id, cig, importo_aggiudicazione, data_aggiudicazione', [aggiudicazioni_row])
        elif source_type == 'partecipanti':
            partecipanti_row = f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('codice_fiscale', ''))}, {sql_escape(record.get('ragione_sociale', ''))}, {sql_escape(record.get('importo_offerto', 0.0))})"
            write_insert_chunk(f, 'partecipanti', 'id, cig, codice_fiscale, ragione_sociale, importo_offerto', [partecipanti_row])
        elif source_type == 'varianti':
            varianti_row = f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('importo_variante', 0.0))}, {sql_escape(record.get('data_variante', ''))})"
            write_insert_chunk(f, 'varianti', 'id, cig, importo_variante, data_variante', [varianti_row])
        
        # Processa CIG
        if source_type in ['bandi', 'aggiudicazioni', 'partecipanti', 'varianti']:
            cig_row = f"({sql_escape(cig)}, {sql_escape(record.get('oggetto', ''))}, {sql_escape(record.get('importo', 0.0))}, {sql_escape(record.get('data_pubblicazione', ''))}, {sql_escape(record.get('data_scadenza', ''))}, {sql_escape(record.get('stato', ''))}, NOW())"
            write_insert_chunk(f, 'cig', 'cig, oggetto, importo, data_pubblicazione, data_scadenza, stato, created_at', [cig_row])

def main():
    parser = argparse.ArgumentParser(description='Esporta dati JSON in formato SQL per MySQL')
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE,
                      help=f'Numero di righe per ogni blocco di INSERT (default: {DEFAULT_CHUNK_SIZE})')
    args = parser.parse_args()
    
    CHUNK_SIZE = args.chunk_size
    start_time = time.time()
    print(f"🕒 Inizio esportazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Utilizzo chunk size di {CHUNK_SIZE} righe")

    with open(SQL_FILE, 'w', encoding='utf-8', buffering=1) as f:  # Line buffering
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
            with open(json_file, 'r', encoding='utf-8') as jf:
                for line in jf:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        file_records += 1
                        total_records += 1
                        
                        # Processa il record immediatamente
                        process_record(record, file_name, source_type, f, CHUNK_SIZE)
                        
                        # Forza garbage collection ogni 1000 record
                        if file_records % 1000 == 0:
                            gc.collect()
                            
                    except Exception as e:
                        print(f"❌ Errore nel parsing o esportazione: {e}")
                        continue
            
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
            
            # Forza garbage collection dopo ogni file
            gc.collect()
    
    total_time = time.time() - start_time
    print(f"\n✨ Esportazione completata!")
    print(f"📊 Statistiche finali:")
    print(f"   • Record totali: {total_records:,}")
    print(f"   • File processati: {total_files}")
    print(f"   • Chunk size: {CHUNK_SIZE:,}")
    print(f"   • Tempo totale: {format_time(total_time)}")
    print(f"   • Velocità media: {total_records/total_time:.1f} record/s")
    print(f"   • File generato: {SQL_FILE}")
    print(f"🕒 Fine esportazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 