from typing import List, Dict, Generator
import json
from log import logger
import time
import psutil
import os
import sqlite3
import pandas as pd
import gc

def get_memory_usage() -> str:
    """Returns current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return f"{process.memory_info().rss / 1024 / 1024:.1f}MB"

def create_table_schema(conn: sqlite3.Connection, table_name: str, first_record: Dict) -> None:
    """Crea lo schema della tabella basato sul primo record."""
    columns = []
    for key, value in first_record.items():
        # Determina il tipo di colonna basato sul valore
        if isinstance(value, int):
            col_type = "INTEGER"
        elif isinstance(value, float):
            col_type = "REAL"
        else:
            col_type = "TEXT"
        columns.append(f"{key} {col_type}")
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    conn.execute(create_sql)
    conn.commit()

def import_json_file(file_path: str, conn: sqlite3.Connection, batch_size: int = 1000) -> None:
    """Importa un file JSONL nel database in modo efficiente."""
    try:
        # Ottieni il nome della tabella dal nome del file
        table_name = os.path.splitext(os.path.basename(file_path))[0].replace('-', '_')
        
        # Conta le righe totali
        with open(file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        
        logger.info(f"📊 File {file_path} contiene {total_lines} righe da processare")
        
        # Disabilita gli indici temporaneamente
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        
        processed_lines = 0
        batch = []
        start_time = time.time()
        last_progress_time = start_time
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    
                    # Se è il primo record, crea la tabella
                    if processed_lines == 0:
                        create_table_schema(conn, table_name, record)
                    
                    # Prepara i valori per l'inserimento
                    values = [record.get(key, None) for key in record.keys()]
                    batch.append(values)
                    
                    processed_lines += 1
                    
                    # Se abbiamo raggiunto la dimensione del batch, inserisci nel database
                    if len(batch) >= batch_size:
                        placeholders = ','.join(['?' for _ in record.keys()])
                        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                        conn.executemany(insert_sql, batch)
                        conn.commit()
                        batch = []
                        
                        # Mostra progresso ogni 5 secondi
                        current_time = time.time()
                        if current_time - last_progress_time >= 5:
                            progress = (processed_lines / total_lines) * 100
                            elapsed = current_time - start_time
                            speed = processed_lines / elapsed if elapsed > 0 else 0
                            logger.info(f"""
⏳ Progresso: {progress:.1f}% ({processed_lines}/{total_lines} righe)
🚀 Velocità: {speed:.1f} righe/secondo
💾 Memoria: {get_memory_usage()}
""")
                            last_progress_time = current_time
                            
                            # Forza garbage collection
                            gc.collect()
                
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Errore nel parsing della riga JSON: {str(e)}")
                    logger.error(f"📝 Contenuto riga problematica: {line[:200]}...")
                    continue
        
        # Inserisci l'ultimo batch se non è vuoto
        if batch:
            placeholders = ','.join(['?' for _ in record.keys()])
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            conn.executemany(insert_sql, batch)
            conn.commit()
        
        # Riabilita gli indici
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        
        # Crea indici per le colonne più utilizzate
        for key in record.keys():
            if key.endswith('_codice') or key.endswith('_id'):
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{key} ON {table_name}({key})")
        
        conn.commit()
        
        total_time = time.time() - start_time
        logger.info(f"""
✅ Importazione completata per {table_name}:
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

def import_all_json_files(base_path: str, db_path: str) -> None:
    """Importa tutti i file JSONL da una directory e dalle sue sottodirectory nel database."""
    try:
        # Crea la connessione al database
        conn = sqlite3.connect(db_path)
        
        # Verifica che la directory esista
        if not os.path.exists(base_path):
            logger.error(f"❌ Directory non trovata: {base_path}")
            return
        
        # Trova tutti i file JSON ricorsivamente
        json_files = find_json_files(base_path)
        
        if not json_files:
            logger.warning(f"⚠️ Nessun file JSON trovato in {base_path} o nelle sue sottodirectory")
            return
        
        logger.info(f"📂 Trovati {len(json_files)} file JSON da importare")
        
        # Importa ogni file
        for json_file in json_files:
            logger.info(f"📄 Elaborazione file: {json_file}")
            import_json_file(json_file, conn)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Errore nell'importazione dei file: {str(e)}")
        raise

if __name__ == "__main__":
    import_all_json_files("/database/JSON", "database.db") 