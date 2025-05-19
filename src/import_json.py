from typing import List, Dict, Generator
import json
from log import logger
import time
import psutil
import os
import sqlite3
import pandas as pd

def get_memory_usage() -> str:
    """Returns current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return f"{process.memory_info().rss / 1024 / 1024:.1f}MB"

def read_json_file(file_path: str, chunk_size: int = 1000) -> Generator[List[Dict], None, None]:
    """Legge un file JSON e restituisce i dati come generator di liste di dizionari.
    
    Args:
        file_path: Percorso del file JSON
        chunk_size: Numero di righe da processare per volta
        
    Yields:
        Lista di dizionari contenenti i dati JSON
        
    Raises:
        FileNotFoundError: Se il file non esiste
        json.JSONDecodeError: Se il file non è un JSON valido
    """
    try:
        # Prima contiamo il numero totale di righe
        with open(file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        
        logger.info(f"📊 File contiene {total_lines} righe da processare")
        logger.info(f"💾 Memoria iniziale: {get_memory_usage()}")
        
        processed_lines = 0
        chunk = []
        last_progress_time = time.time()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        json_obj = json.loads(line)
                        chunk.append(json_obj)
                        processed_lines += 1
                        
                        # Se abbiamo raggiunto la dimensione del chunk, lo restituiamo
                        if len(chunk) >= chunk_size:
                            yield chunk
                            chunk = []
                            
                            # Mostra progresso ogni 5 secondi o ogni 1000 righe
                            current_time = time.time()
                            if current_time - last_progress_time >= 5:
                                progress = (processed_lines / total_lines) * 100
                                logger.info(f"⏳ Progresso: {progress:.1f}% ({processed_lines}/{total_lines} righe)")
                                logger.info(f"💾 Memoria attuale: {get_memory_usage()}")
                                last_progress_time = current_time
                                
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ Errore nel parsing della riga JSON: {str(e)}")
                        continue
            
            # Restituisci l'ultimo chunk se non è vuoto
            if chunk:
                yield chunk
                
        logger.info(f"✅ Processamento completato: {processed_lines} righe elaborate")
        logger.info(f"💾 Memoria finale: {get_memory_usage()}")
        
    except FileNotFoundError:
        logger.error(f"❌ File non trovato: {file_path}")
        raise
    except Exception as e:
        logger.error(f"❌ Errore nella lettura del file {file_path}: {str(e)}")
        raise

def import_json_folder(folder_path: str, conn: sqlite3.Connection) -> None:
    """Importa tutti i file JSON da una cartella nel database.
    
    Args:
        folder_path: Percorso della cartella contenente i file JSON
        conn: Connessione al database SQLite
    """
    try:
        # Verifica che la cartella esista
        if not os.path.exists(folder_path):
            logger.error(f"❌ Cartella non trovata: {folder_path}")
            return
            
        # Ottieni il nome della cartella per il nome della tabella
        folder_name = os.path.basename(folder_path)
        table_name = folder_name.replace('-', '_')
        
        # Trova tutti i file JSON nella cartella
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        
        if not json_files:
            logger.warning(f"⚠️ Nessun file JSON trovato in {folder_path}")
            return
            
        logger.info(f"📂 Elaborazione {len(json_files)} file in {folder_name}")
        
        # Crea la tabella se non esiste
        create_table_if_not_exists(conn, table_name)
        
        total_rows = 0
        for json_file in json_files:
            file_path = os.path.join(folder_path, json_file)
            try:
                # Leggi il file JSON in chunks
                for chunk in read_json_file(file_path):
                    if chunk:
                        # Converti il chunk in DataFrame
                        df = pd.DataFrame(chunk)
                        
                        # Importa il chunk nel database
                        df.to_sql(table_name, conn, if_exists='append', index=False)
                        total_rows += len(chunk)
                        
                        # Commit dopo ogni chunk
                        conn.commit()
                        
                logger.info(f"✅ Importata tabella '{table_name}' con {total_rows} righe")
                
            except Exception as e:
                logger.error(f"❌ Errore nell'importazione del file {json_file}: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"❌ Errore nell'importazione della cartella {folder_path}: {str(e)}")
        raise 