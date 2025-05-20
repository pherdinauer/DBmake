from typing import List, Dict, Generator
import json
import os
import sys

# Aggiungi la directory corrente al path di Python
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from log import logger
import time
import psutil
import sqlite3
import pandas as pd
import gc

def get_memory_usage() -> str:
    """Returns current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return f"{process.memory_info().rss / 1024 / 1024:.1f}MB"

def create_database_schema(conn: sqlite3.Connection) -> None:
    """Crea lo schema del database unificato e la tabella raw_import."""
    # Tabella principale per i CIG
    conn.execute("""
    CREATE TABLE IF NOT EXISTS cig (
        cig TEXT PRIMARY KEY,
        oggetto TEXT,
        importo REAL,
        data_pubblicazione TEXT,
        data_scadenza TEXT,
        stato TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Tabella per i bandi
    conn.execute("""
    CREATE TABLE IF NOT EXISTS bandi (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cig TEXT,
        tipo_bando TEXT,
        modalita_realizzazione TEXT,
        tipo_scelta_contraente TEXT,
        FOREIGN KEY (cig) REFERENCES cig(cig)
    )
    """)
    
    # Tabella per le aggiudicazioni
    conn.execute("""
    CREATE TABLE IF NOT EXISTS aggiudicazioni (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cig TEXT,
        importo_aggiudicazione REAL,
        data_aggiudicazione TEXT,
        FOREIGN KEY (cig) REFERENCES cig(cig)
    )
    """)
    
    # Tabella per i partecipanti
    conn.execute("""
    CREATE TABLE IF NOT EXISTS partecipanti (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cig TEXT,
        codice_fiscale TEXT,
        ragione_sociale TEXT,
        importo_offerto REAL,
        FOREIGN KEY (cig) REFERENCES cig(cig)
    )
    """)
    
    # Tabella per le varianti
    conn.execute("""
    CREATE TABLE IF NOT EXISTS varianti (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cig TEXT,
        importo_variante REAL,
        data_variante TEXT,
        FOREIGN KEY (cig) REFERENCES cig(cig)
    )
    """)
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_import (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cig TEXT,
        raw_json TEXT,
        source_file TEXT
    )
    """)
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS import_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file TEXT UNIQUE,
        import_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    conn.commit()

def extract_cig_data(record: Dict) -> Dict:
    """Estrae i dati rilevanti dal record JSON."""
    cig_data = {
        'cig': record.get('cig', ''),
        'oggetto': record.get('oggetto', ''),
        'importo': record.get('importo', 0.0),
        'data_pubblicazione': record.get('data_pubblicazione', ''),
        'data_scadenza': record.get('data_scadenza', ''),
        'stato': record.get('stato', '')
    }
    return cig_data

def process_record(conn: sqlite3.Connection, record: Dict, source_type: str) -> None:
    """Processa un singolo record e lo inserisce nelle tabelle appropriate."""
    try:
        cig = record.get('cig', '')
        if not cig:
            return
            
        # Inserisci o aggiorna il CIG principale
        cig_data = extract_cig_data(record)
        conn.execute("""
        INSERT OR REPLACE INTO cig (cig, oggetto, importo, data_pubblicazione, data_scadenza, stato)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            cig_data['cig'],
            cig_data['oggetto'],
            cig_data['importo'],
            cig_data['data_pubblicazione'],
            cig_data['data_scadenza'],
            cig_data['stato']
        ))
        
        # Inserisci i dati specifici in base al tipo di record
        if source_type == 'bandi':
            conn.execute("""
            INSERT INTO bandi (cig, tipo_bando, modalita_realizzazione, tipo_scelta_contraente)
            VALUES (?, ?, ?, ?)
            """, (
                cig,
                record.get('tipo_bando', ''),
                record.get('modalita_realizzazione', ''),
                record.get('tipo_scelta_contraente', '')
            ))
            
        elif source_type == 'aggiudicazioni':
            conn.execute("""
            INSERT INTO aggiudicazioni (cig, importo_aggiudicazione, data_aggiudicazione)
            VALUES (?, ?, ?)
            """, (
                cig,
                record.get('importo_aggiudicazione', 0.0),
                record.get('data_aggiudicazione', '')
            ))
            
        elif source_type == 'partecipanti':
            conn.execute("""
            INSERT INTO partecipanti (cig, codice_fiscale, ragione_sociale, importo_offerto)
            VALUES (?, ?, ?, ?)
            """, (
                cig,
                record.get('codice_fiscale', ''),
                record.get('ragione_sociale', ''),
                record.get('importo_offerto', 0.0)
            ))
            
        elif source_type == 'varianti':
            conn.execute("""
            INSERT INTO varianti (cig, importo_variante, data_variante)
            VALUES (?, ?, ?)
            """, (
                cig,
                record.get('importo_variante', 0.0),
                record.get('data_variante', '')
            ))
            
    except Exception as e:
        logger.error(f"❌ Errore nel processare il record: {str(e)}")
        logger.error(f"📝 Record problematico: {record}")

def import_json_file(file_path: str, conn: sqlite3.Connection, batch_size: int = 1000) -> None:
    """Importa un file JSONL nel database unificato e nella tabella raw_import."""
    batch = []
    processed_lines = 0
    start_time = time.time()
    last_progress_time = start_time
    try:
        file_name = os.path.basename(file_path)
        source_type = file_name.split('_')[0]
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    cig = record.get('cig', None)
                    # Inserisci nella tabella raw_import
                    conn.execute(
                        "INSERT INTO raw_import (cig, raw_json, source_file) VALUES (?, ?, ?)",
                        (cig, json.dumps(record, ensure_ascii=False), file_name)
                    )
                    batch.append(record)
                    processed_lines += 1
                    if len(batch) >= batch_size:
                        for record in batch:
                            process_record(conn, record, source_type)
                        conn.commit()
                        batch = []
                        current_time = time.time()
                        if current_time - last_progress_time >= 5:
                            elapsed = current_time - start_time
                            speed = processed_lines / elapsed if elapsed > 0 else 0
                            logger.info(f"""
⏳ Progresso: {processed_lines} righe
🚀 Velocità: {speed:.1f} righe/secondo
💾 Memoria: {get_memory_usage()}
""")
                            last_progress_time = current_time
                            gc.collect()
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Errore nel parsing della riga JSON: {str(e)}")
                    logger.error(f"📝 Contenuto riga problematica: {line[:200]}...")
                    continue
        # Processa l'ultimo batch se non è vuoto
        if batch:
            for record in batch:
                process_record(conn, record, source_type)
            conn.commit()
        
        # Crea indici per le colonne più utilizzate
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cig ON cig(cig)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bandi_cig ON bandi(cig)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_aggiudicazioni_cig ON aggiudicazioni(cig)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_partecipanti_cig ON partecipanti(cig)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_varianti_cig ON varianti(cig)")
        
        conn.commit()
        
        total_time = time.time() - start_time
        logger.info(f"""
✅ Importazione completata per {file_path}:
   - Righe elaborate: {processed_lines:,}
   - Tempo totale: {total_time:.1f} secondi
   - Velocità media: {processed_lines/total_time:.1f} righe/secondo
   - Memoria finale: {get_memory_usage()}
""")
        
    except Exception as e:
        logger.error(f"❌ Errore nell'importazione del file {file_path}: {str(e)}")
        raise

def find_json_files(base_path: str) -> List[str]:
    """Trova ricorsivamente tutti i file JSON in una directory e nelle sue sottodirectory."""
    json_files = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    return json_files

def import_all_json_files(base_path: str, db_path: str, batch_size: int = None) -> None:
    """Importa tutti i file JSONL da una directory e dalle sue sottodirectory nel database unificato."""
    try:
        # Crea la connessione al database
        conn = sqlite3.connect(db_path)
        
        # Crea lo schema del database
        create_database_schema(conn)
        
        # Verifica che la directory esista
        if not os.path.exists(base_path):
            logger.error(f"❌ Directory non trovata: {base_path}")
            return
        
        # Trova tutti i file JSON ricorsivamente
        json_files = find_json_files(base_path)
        total_files = len(json_files)
        
        if not json_files:
            logger.warning(f"⚠️ Nessun file JSON trovato in {base_path} o nelle sue sottodirectory")
            return
        
        logger.info(f"📂 Trovati {total_files} file JSON da importare")
        
        # Statistiche di importazione
        total_records = 0
        start_time = time.time()
        
        # Batch size configurabile
        if batch_size is None:
            batch_size = int(os.environ.get("IMPORT_BATCH_SIZE", 1000))
        logger.info(f"🚀 Batch size impostato a {batch_size}")
        
        # Recupera i file già importati
        imported_files = set(row[0] for row in conn.execute("SELECT source_file FROM import_log").fetchall())
        
        # Importa ogni file con avanzamento globale
        for idx, json_file in enumerate(json_files, 1):
            file_name = os.path.basename(json_file)
            percent = (idx / total_files) * 100
            if file_name in imported_files:
                logger.info(f"[{idx:>3}/{total_files}] ({percent:.1f}%) File già importato, salto: {json_file}")
                continue
            logger.info(f"[{idx:>3}/{total_files}] ({percent:.1f}%) Elaborazione file: {json_file}")
            import_json_file(json_file, conn, batch_size=batch_size)
            
            # Conta i record importati
            source_type = file_name.split('_')[0]
            cursor = conn.cursor()
            
            if source_type == 'bandi':
                cursor.execute("SELECT COUNT(*) FROM bandi")
            elif source_type == 'aggiudicazioni':
                cursor.execute("SELECT COUNT(*) FROM aggiudicazioni")
            elif source_type == 'partecipanti':
                cursor.execute("SELECT COUNT(*) FROM partecipanti")
            elif source_type == 'varianti':
                cursor.execute("SELECT COUNT(*) FROM varianti")
                
            result = cursor.fetchone()
            if result is not None:
                count = result[0]
            else:
                count = 0
            total_records += count
            # Registra il file come importato
            conn.execute("INSERT OR IGNORE INTO import_log (source_file) VALUES (?)", (file_name,))
            conn.commit()
        
        # Calcola statistiche finali
        total_time = time.time() - start_time
        result = conn.execute("SELECT COUNT(DISTINCT cig) FROM cig").fetchone()
        if result is not None:
            total_cig = result[0]
        else:
            total_cig = 0
        
        logger.info(f"""
✅ Importazione completata con successo!

📊 Riepilogo:
   - File elaborati: {total_files}
   - Record totali: {total_records:,}
   - CIG unici: {total_cig:,}
   - Tempo totale: {total_time:.1f} secondi
   - Velocità media: {total_records/total_time:.1f} record/secondo
   - Memoria finale: {get_memory_usage()}

📁 Database salvato in: {os.path.abspath(db_path)}
📝 Log disponibili in: {os.path.abspath('logs')}
""")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Errore nell'importazione dei file: {str(e)}")
        raise

if __name__ == "__main__":
    # Permetti override batch_size da variabile d'ambiente
    batch_size = int(os.environ.get("IMPORT_BATCH_SIZE", 1000))
    import_all_json_files("/database/JSON", "database.db", batch_size=batch_size) 