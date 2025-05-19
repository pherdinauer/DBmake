from typing import List, Dict
import json
from log import logger
import time

def read_json_file(file_path: str) -> List[Dict]:
    """Legge un file JSON e restituisce i dati come lista di dizionari.
    
    Args:
        file_path: Percorso del file JSON
        
    Returns:
        Lista di dizionari contenenti i dati JSON
        
    Raises:
        FileNotFoundError: Se il file non esiste
        json.JSONDecodeError: Se il file non è un JSON valido
    """
    try:
        data = []
        total_lines = 0
        processed_lines = 0
        last_progress_time = time.time()
        
        # Prima contiamo il numero totale di righe
        with open(file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        
        logger.info(f"📊 File contiene {total_lines} righe da processare")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        json_obj = json.loads(line)
                        data.append(json_obj)
                        processed_lines += 1
                        
                        # Mostra progresso ogni 5 secondi o ogni 1000 righe
                        current_time = time.time()
                        if current_time - last_progress_time >= 5 or processed_lines % 1000 == 0:
                            progress = (processed_lines / total_lines) * 100
                            logger.info(f"⏳ Progresso: {progress:.1f}% ({processed_lines}/{total_lines} righe)")
                            last_progress_time = current_time
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ Errore nel parsing della riga JSON: {str(e)}")
                        continue
                        
        logger.info(f"✅ Processamento completato: {processed_lines} righe elaborate")
        return data
    except FileNotFoundError:
        logger.error(f"❌ File non trovato: {file_path}")
        raise
    except Exception as e:
        logger.error(f"❌ Errore nella lettura del file {file_path}: {str(e)}")
        raise 