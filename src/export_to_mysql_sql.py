import os
import json
import glob

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

def main():
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
        print(f"Trovati {total_files} file JSON da esportare")
        for idx, json_file in enumerate(json_files, 1):
            print(f"[{idx:>3}/{total_files}] Esporto file: {json_file}")
            file_name = os.path.basename(json_file)
            source_type = file_name.split('_')[0]
            with open(json_file, 'r', encoding='utf-8') as jf:
                for line in jf:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        cig = record.get('cig', '')
                        # raw_import
                        raw_import_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(json.dumps(record, ensure_ascii=False))}, {sql_escape(file_name)})")
                        # relazionali
                        if cig:
                            if source_type == 'bandi':
                                bandi_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('tipo_bando', ''))}, {sql_escape(record.get('modalita_realizzazione', ''))}, {sql_escape(record.get('tipo_scelta_contraente', ''))})")
                            elif source_type == 'aggiudicazioni':
                                aggiudicazioni_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('importo_aggiudicazione', 0.0))}, {sql_escape(record.get('data_aggiudicazione', ''))})")
                            elif source_type == 'partecipanti':
                                partecipanti_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('codice_fiscale', ''))}, {sql_escape(record.get('ragione_sociale', ''))}, {sql_escape(record.get('importo_offerto', 0.0))})")
                            elif source_type == 'varianti':
                                varianti_rows.append(f"(NULL, {sql_escape(cig)}, {sql_escape(record.get('importo_variante', 0.0))}, {sql_escape(record.get('data_variante', ''))})")
                            # cig
                            if source_type in ['bandi', 'aggiudicazioni', 'partecipanti', 'varianti']:
                                cig_row = f"({sql_escape(cig)}, {sql_escape(record.get('oggetto', ''))}, {sql_escape(record.get('importo', 0.0))}, {sql_escape(record.get('data_pubblicazione', ''))}, {sql_escape(record.get('data_scadenza', ''))}, {sql_escape(record.get('stato', ''))}, NOW())"
                                cig_rows.append(cig_row)
                    except Exception as e:
                        print(f"Errore nel parsing o esportazione: {e}")
                        continue
        # Scrivi INSERT INTO
        if cig_rows:
            f.write("INSERT IGNORE INTO cig (cig, oggetto, importo, data_pubblicazione, data_scadenza, stato, created_at) VALUES\n  " + ',\n  '.join(cig_rows) + ";\n\n")
        if bandi_rows:
            f.write("INSERT INTO bandi (id, cig, tipo_bando, modalita_realizzazione, tipo_scelta_contraente) VALUES\n  " + ',\n  '.join(bandi_rows) + ";\n\n")
        if aggiudicazioni_rows:
            f.write("INSERT INTO aggiudicazioni (id, cig, importo_aggiudicazione, data_aggiudicazione) VALUES\n  " + ',\n  '.join(aggiudicazioni_rows) + ";\n\n")
        if partecipanti_rows:
            f.write("INSERT INTO partecipanti (id, cig, codice_fiscale, ragione_sociale, importo_offerto) VALUES\n  " + ',\n  '.join(partecipanti_rows) + ";\n\n")
        if varianti_rows:
            f.write("INSERT INTO varianti (id, cig, importo_variante, data_variante) VALUES\n  " + ',\n  '.join(varianti_rows) + ";\n\n")
        if raw_import_rows:
            f.write("INSERT INTO raw_import (id, cig, raw_json, source_file) VALUES\n  " + ',\n  '.join(raw_import_rows) + ";\n\n")
    print(f"✅ File SQL generato: {SQL_FILE}")

if __name__ == "__main__":
    main() 