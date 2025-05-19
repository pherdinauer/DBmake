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

def read_json_file(file_path: str, chunk_size: int = 100) -> Generator[List[Dict], None, None]:
    """Legge un file JSONL (JSON Lines) e restituisce i dati come generator di liste di dizionari.
    
    Args:
        file_path: Percorso del file JSONL
        chunk_size: Numero di righe da processare per volta
        
    Yields:
        Lista di dizionari contenenti i dati JSON
        
    Raises:
        FileNotFoundError: Se il file non esiste
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
        error_count = 0
        max_errors = 1000  # Numero massimo di errori prima di interrompere
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:  # Skip empty lines
                    continue
                    
                try:
                    json_obj = json.loads(line)
                    chunk.append(json_obj)
                    processed_lines += 1
                    
                    # Se abbiamo raggiunto la dimensione del chunk, lo restituiamo
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                        
                        # Forza garbage collection dopo ogni chunk
                        import gc
                        gc.collect()
                        
                        # Mostra progresso ogni 5 secondi o ogni 1000 righe
                        current_time = time.time()
                        if current_time - last_progress_time >= 5:
                            progress = (processed_lines / total_lines) * 100
                            memory_usage = get_memory_usage()
                            logger.info(f"""
⏳ Progresso: {progress:.1f}% ({processed_lines}/{total_lines} righe)
💾 Memoria attuale: {memory_usage}
❌ Errori: {error_count}
""")
                            last_progress_time = current_time
                            
                            # Se la memoria è troppo alta, forza una pausa
                            if float(memory_usage.replace('MB', '')) > 1000:  # 1GB
                                logger.warning("⚠️ Memoria elevata, pausa di 5 secondi...")
                                time.sleep(5)
                                gc.collect()
                            
                except json.JSONDecodeError as e:
                    error_count += 1
                    logger.error(f"❌ Errore nel parsing della riga JSON: {str(e)}")
                    logger.error(f"📝 Contenuto riga problematica: {line[:200]}...")  # Mostra i primi 200 caratteri della riga
                    if error_count >= max_errors:
                        logger.error(f"❌ Troppi errori ({error_count}), interruzione elaborazione")
                        break
                    continue
        
        # Restituisci l'ultimo chunk se non è vuoto
        if chunk:
            yield chunk
            
        logger.info(f"""
✅ Processamento completato:
   - Righe elaborate: {processed_lines}
   - Errori riscontrati: {error_count}
   - Memoria finale: {get_memory_usage()}
""")
        
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
        start_time = time.time()
        last_progress_time = start_time
        last_rows = 0
        
        for json_file in json_files:
            file_path = os.path.join(folder_path, json_file)
            file_start_time = time.time()
            file_rows = 0
            
            try:
                logger.info(f"📄 Elaborazione file: {json_file}")
                
                # Leggi il file JSON in chunks
                for chunk in read_json_file(file_path):
                    if chunk:
                        # Converti il chunk in DataFrame
                        df = pd.DataFrame(chunk)
                        
                        # Importa il chunk nel database
                        df.to_sql(table_name, conn, if_exists='append', index=False)
                        chunk_rows = len(chunk)
                        total_rows += chunk_rows
                        file_rows += chunk_rows
                        
                        # Commit dopo ogni chunk
                        conn.commit()
                        
                        # Calcola e mostra statistiche ogni 5 secondi
                        current_time = time.time()
                        if current_time - last_progress_time >= 5:
                            elapsed = current_time - start_time
                            rows_per_second = total_rows / elapsed if elapsed > 0 else 0
                            memory_usage = get_memory_usage()
                            
                            logger.info(f"""
📊 Statistiche di importazione:
   - Righe elaborate: {total_rows:,}
   - Velocità: {rows_per_second:.1f} righe/secondo
   - Memoria utilizzata: {memory_usage}
   - Tempo trascorso: {elapsed:.1f} secondi
""")
                            last_progress_time = current_time
                            last_rows = total_rows
                
                file_elapsed = time.time() - file_start_time
                logger.info(f"✅ File completato: {json_file} ({file_rows:,} righe in {file_elapsed:.1f} secondi)")
                
            except Exception as e:
                logger.error(f"❌ Errore nell'importazione del file {json_file}: {str(e)}")
                continue
        
        total_elapsed = time.time() - start_time
        logger.info(f"""
🎉 Importazione completata per {folder_name}:
   - Totale righe: {total_rows:,}
   - Tempo totale: {total_elapsed:.1f} secondi
   - Velocità media: {total_rows/total_elapsed:.1f} righe/secondo
   - Memoria finale: {get_memory_usage()}
""")
                
    except Exception as e:
        logger.error(f"❌ Errore nell'importazione della cartella {folder_path}: {str(e)}")
        raise 