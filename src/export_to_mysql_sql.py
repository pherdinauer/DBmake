import os
import json
import glob
import argparse
import time
from datetime import datetime, timedelta

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

def write_insert_chunk(f, table, columns, rows):
    if not rows:
        return
    f.write(f"INSERT INTO {table} ({columns}) VALUES\n  " + ',\n  '.join(rows) + ";\n\n")

def format_time(seconds):
    return str(timedelta(seconds=int(seconds)))

def main():
    parser = argparse.ArgumentParser(description='Esporta dati JSON in formato SQL per MySQL')
    parser.add_argument('--chunk-size', type=int, default=10000,
                      help='Numero di righe per ogni blocco di INSERT (default: 10000)')
    args = parser.parse_args()
    
    CHUNK_SIZE = args.chunk_size
    start_time = time.time()
    print(f"🕒 Inizio esportazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Utilizzo chunk size di {CHUNK_SIZE} righe")

    with open(SQL_FILE, 'w', encoding='utf-8') as f:
        # CREATE DATABASE
        f.write(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` DEFAULT CHARACTER SET utf8mb4;\nUSE `{MYSQL_DATABASE}`;\n\n")
        # CREATE TABLES
        for ddl in CREATE_TABLES.values():
            f.write(ddl + '\n')
        f.write('\n')

        # Dati relazionali
        cig_rows = []
        bandi_rows = []
        aggiudicazioni_rows = []
        partecipanti_rows = []
        varianti_rows = []
        raw_import_rows = []

        json_files = find_json_files(JSON_BASE_PATH)
        total_files = len(json_files)
        print(f"📁 Trovati {total_files} file JSON da esportare")
        
        total_records = 0
        for idx, json_file in enumerate(json_files, 1):
            file_start_time = time.time()
            print(f"\n[{idx:>3}/{total_files}] 📄 Esporto file: {json_file}")
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
                        
                        # Calcola e mostra progresso ogni 1000 record
                        if file_records % 1000 == 0:
                            elapsed = time.time() - start_time
                            records_per_second = total_records / elapsed
                            eta = (total_files - idx) * (file_records / (time.time() - file_start_time))
                            print(f"   ⏳ Progresso: {file_records:,} record | Velocità: {records_per_second:.1f} record/s | ETA: {format_time(eta)}", end='\r')
                        
                        cig = record.get('cig', '')
                        # raw_import
                        raw_import_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(json.dumps(record, ensure_ascii=False))}, {sql_escape(file_name)})")
                        if len(raw_import_rows) >= CHUNK_SIZE:
                            write_insert_chunk(f, 'raw_import', 'id, cig, raw_json, source_file', raw_import_rows)
                            raw_import_rows = []
                        # relazionali
                        if cig:
                            if source_type == 'bandi':
                                bandi_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('tipo_bando', ''))}, {sql_escape(record.get('modalita_realizzazione', ''))}, {sql_escape(record.get('tipo_scelta_contraente', ''))})")
                                if len(bandi_rows) >= CHUNK_SIZE:
                                    write_insert_chunk(f, 'bandi', 'id, cig, tipo_bando, modalita_realizzazione, tipo_scelta_contraente', bandi_rows)
                                    bandi_rows = []
                            elif source_type == 'aggiudicazioni':
                                aggiudicazioni_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('importo_aggiudicazione', 0.0))}, {sql_escape(record.get('data_aggiudicazione', ''))})")
                                if len(aggiudicazioni_rows) >= CHUNK_SIZE:
                                    write_insert_chunk(f, 'aggiudicazioni', 'id, cig, importo_aggiudicazione, data_aggiudicazione', aggiudicazioni_rows)
                                    aggiudicazioni_rows = []
                            elif source_type == 'partecipanti':
                                partecipanti_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('codice_fiscale', ''))}, {sql_escape(record.get('ragione_sociale', ''))}, {sql_escape(record.get('importo_offerto', 0.0))})")
                                if len(partecipanti_rows) >= CHUNK_SIZE:
                                    write_insert_chunk(f, 'partecipanti', 'id, cig, codice_fiscale, ragione_sociale, importo_offerto', partecipanti_rows)
                                    partecipanti_rows = []
                            elif source_type == 'varianti':
                                varianti_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('importo_variante', 0.0))}, {sql_escape(record.get('data_variante', ''))})")
                                if len(varianti_rows) >= CHUNK_SIZE:
                                    write_insert_chunk(f, 'varianti', 'id, cig, importo_variante, data_variante', varianti_rows)
                                    varianti_rows = []
                            # cig
                            if source_type in ['bandi', 'aggiudicazioni', 'partecipanti', 'varianti']:
                                cig_row = f"({sql_escape(cig)}, {sql_escape(record.get('oggetto', ''))}, {sql_escape(record.get('importo', 0.0))}, {sql_escape(record.get('data_pubblicazione', ''))}, {sql_escape(record.get('data_scadenza', ''))}, {sql_escape(record.get('stato', ''))}, NOW())"
                                cig_rows.append(cig_row)
                                if len(cig_rows) >= CHUNK_SIZE:
                                    write_insert_chunk(f, 'cig', 'cig, oggetto, importo, data_pubblicazione, data_scadenza, stato, created_at', cig_rows)
                                    cig_rows = []
                    except Exception as e:
                        print(f"❌ Errore nel parsing o esportazione: {e}")
                        continue
            
            file_time = time.time() - file_start_time
            print(f"\n   ✅ File completato: {file_records:,} record in {format_time(file_time)}")
        
        # Scrivi gli ultimi chunk rimasti
        write_insert_chunk(f, 'cig', 'cig, oggetto, importo, data_pubblicazione, data_scadenza, stato, created_at', cig_rows)
        write_insert_chunk(f, 'bandi', 'id, cig, tipo_bando, modalita_realizzazione, tipo_scelta_contraente', bandi_rows)
        write_insert_chunk(f, 'aggiudicazioni', 'id, cig, importo_aggiudicazione, data_aggiudicazione', aggiudicazioni_rows)
        write_insert_chunk(f, 'partecipanti', 'id, cig, codice_fiscale, ragione_sociale, importo_offerto', partecipanti_rows)
        write_insert_chunk(f, 'varianti', 'id, cig, importo_variante, data_variante', varianti_rows)
        write_insert_chunk(f, 'raw_import', 'id, cig, raw_json, source_file', raw_import_rows)
    
    total_time = time.time() - start_time
    print(f"\n✨ Esportazione completata!")
    print(f"📊 Statistiche:")
    print(f"   • Record totali: {total_records:,}")
    print(f"   • File processati: {total_files}")
    print(f"   • Chunk size: {CHUNK_SIZE:,}")
    print(f"   • Tempo totale: {format_time(total_time)}")
    print(f"   • Velocità media: {total_records/total_time:.1f} record/s")
    print(f"   • File generato: {SQL_FILE}")
    print(f"🕒 Fine esportazione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 