import os
import json
import time
import gc
import glob
import mysql.connector
from mysql.connector import errorcode

# Parametri di connessione da variabili d'ambiente
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'anac_import')
JSON_BASE_PATH = os.environ.get('ANAC_BASE_PATH', '/database/JSON')
BATCH_SIZE = int(os.environ.get('IMPORT_BATCH_SIZE', 50000))

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

def import_all_json_files(base_path, conn, batch_size=BATCH_SIZE):
    json_files = find_json_files(base_path)
    total_files = len(json_files)
    print(f"Trovati {total_files} file JSON da importare")
    cursor = conn.cursor()
    for idx, json_file in enumerate(json_files, 1):
        percent = (idx / total_files) * 100
        print(f"[{idx:>3}/{total_files}] ({percent:.1f}%) Elaborazione file: {json_file}")
        file_name = os.path.basename(json_file)
        processed_lines = 0
        batch = []
        with open(json_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    cig = record.get('cig', None)
                    # Inserisci nella tabella raw_import
                    cursor.execute(
                        "INSERT INTO raw_import (cig, raw_json, source_file) VALUES (%s, %s, %s)",
                        (cig, json.dumps(record, ensure_ascii=False), file_name)
                    )
                    batch.append((record, file_name))
                    processed_lines += 1
                    if len(batch) >= batch_size:
                        for rec, _ in batch:
                            process_record(cursor, rec, file_name.split('_')[0])
                        conn.commit()
                        batch = []
                        print(f"  ... {processed_lines} righe elaborate")
                        gc.collect()
                except Exception as e:
                    print(f"Errore nel parsing o inserimento: {e}")
                    continue
        # Processa l'ultimo batch
        if batch:
            for rec, _ in batch:
                process_record(cursor, rec, file_name.split('_')[0])
            conn.commit()
        print(f"✅ File {json_file} importato con successo ({processed_lines} righe)")
    cursor.close()
    print("✅ Importazione completata!")

def main():
    conn = connect_mysql()
    create_tables(conn)
    import_all_json_files(JSON_BASE_PATH, conn, batch_size=BATCH_SIZE)
    conn.close()
    print("Tutte le connessioni chiuse.")

if __name__ == "__main__":
    main() 