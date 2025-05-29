"""
System information e monitoring utilities.
"""

import os
import psutil
import logging
import multiprocessing
from typing import Dict, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

def get_system_resources() -> Dict[str, float]:
    """Ottiene informazioni sulle risorse del sistema."""
    memory_info = psutil.virtual_memory()
    
    return {
        'cpu_cores': multiprocessing.cpu_count(),
        'total_memory_gb': memory_info.total / (1024**3),
        'available_memory_gb': memory_info.available / (1024**3),
        'used_memory_gb': memory_info.used / (1024**3),
        'memory_percent': memory_info.percent,
        'disk_usage_percent': psutil.disk_usage('/').percent if os.name != 'nt' else psutil.disk_usage('C:').percent
    }


def check_disk_space() -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Verifica lo spazio disponibile su disco usando pathlib."""
    try:
        # Verifica lo spazio su /database
        database_path = Path('/database')
        if database_path.exists():
            database_stats = os.statvfs(str(database_path))
            database_free_gb = (database_stats.f_bavail * database_stats.f_frsize) / (1024**3)
        else:
            database_free_gb = 0
        
        # Verifica lo spazio su /tmp
        tmp_path = Path('/tmp')
        if tmp_path.exists():
            tmp_stats = os.statvfs(str(tmp_path))
            tmp_free_gb = (tmp_stats.f_bavail * tmp_stats.f_frsize) / (1024**3)
        else:
            tmp_free_gb = 0
        
        # Verifica lo spazio su /database/tmp
        tmp_dir_path = Path('/database/tmp')
        if tmp_dir_path.exists():
            tmp_dir_stats = os.statvfs(str(tmp_dir_path))
            tmp_dir_free_gb = (tmp_dir_stats.f_bavail * tmp_dir_stats.f_frsize) / (1024**3)
        else:
            tmp_dir_free_gb = 0
        
        logger.info("Spazio disco disponibile:")
        logger.info(f"   📁 {database_path}: {database_free_gb:.1f}GB")
        logger.info(f"   📁 {tmp_path}: {tmp_free_gb:.1f}GB")
        logger.info(f"   📁 {tmp_dir_path}: {tmp_dir_free_gb:.1f}GB")
        
        # Avvisa se lo spazio è basso
        if database_free_gb < 10:
            logger.warning(f"⚠️ Spazio disponibile su {database_path} è basso!")
        if tmp_free_gb < 1:
            logger.warning(f"⚠️ Spazio disponibile su {tmp_path} è basso!")
        if tmp_dir_free_gb < 1:
            logger.warning(f"⚠️ Spazio disponibile su {tmp_dir_path} è basso!")
            
        return database_free_gb, tmp_free_gb, tmp_dir_free_gb
    except Exception as e:
        logger.error(f"❌ Errore nel controllo dello spazio disco: {e}")
        return None, None, None


def calculate_optimal_chunk_size(target_memory_usage: float = 0.8) -> int:
    """Calcola la dimensione ottimale del chunk basata sulla memoria disponibile."""
    memory_info = psutil.virtual_memory()
    available_memory = memory_info.available
    target_memory = available_memory * target_memory_usage
    
    # Stima di 2KB per record
    estimated_record_size = 2 * 1024
    chunk_size = int(target_memory / estimated_record_size)
    
    # Limiti di sicurezza
    min_chunk = 5000
    max_chunk = 150000
    
    return max(min_chunk, min(chunk_size, max_chunk)) 