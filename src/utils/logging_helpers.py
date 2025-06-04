"""
Helper functions per logging strutturato e context management.
"""

import time
import logging
import psutil
from typing import Optional, Any

class LogContext:
    """Context manager per logging automatico di inizio/fine operazioni."""
    
    def __init__(self, logger_instance: logging.Logger, operation_name: str, **context: Any) -> None:
        self.logger = logger_instance
        self.operation_name = operation_name
        self.context = context
        self.start_time: Optional[float] = None
    
    def __enter__(self) -> 'LogContext':
        self.start_time = time.time()
        context_str = " | ".join(f"{k}={v}" for k, v in self.context.items()) if self.context else ""
        self.logger.info(f"[START] Inizio {self.operation_name}" + (f" ({context_str})" if context_str else ""))
        return self
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        elapsed = time.time() - (self.start_time or 0)
        if exc_type is None:
            self.logger.info(f"[COMPLETE] Completato {self.operation_name} in {elapsed:.1f}s")
        else:
            self.logger.error(f"[ERROR] Errore in {self.operation_name} dopo {elapsed:.1f}s: {exc_val}")


def log_memory_status(logger_instance: logging.Logger, context: str = "") -> None:
    """Helper per logging status memoria."""
    memory_info = psutil.virtual_memory()
    used_gb = memory_info.used / (1024**3)
    total_gb = memory_info.total / (1024**3)
    usage_pct = memory_info.percent
    available_gb = memory_info.available / (1024**3)
    
    prefix = f"[{context}] " if context else ""
    logger_instance.info(f"[RAM] {prefix}RAM: {used_gb:.1f}GB/{total_gb:.1f}GB ({usage_pct:.1f}%) | Disponibile: {available_gb:.1f}GB")


def log_performance_stats(logger_instance: logging.Logger, operation: str, count: int, elapsed_time: float, context: str = "") -> None:
    """Helper per logging statistiche performance."""
    speed = count / elapsed_time if elapsed_time > 0 else 0
    prefix = f"[{context}] " if context else ""
    logger_instance.info(f"[PERF] {prefix}{operation}: {count:,} elementi in {elapsed_time:.1f}s ({speed:.1f} el/s)")


def log_file_progress(logger_instance: logging.Logger, current: int, total: int, file_name: str = "", extra_info: str = "") -> None:
    """Helper per logging progresso file."""
    pct = (current / total * 100) if total > 0 else 0
    file_info = f" - {file_name}" if file_name else ""
    extra = f" | {extra_info}" if extra_info else ""
    logger_instance.info(f"[PROG] Progresso: {current}/{total} ({pct:.1f}%){file_info}{extra}")


def log_batch_progress(logger_instance: logging.Logger, processed: int, total: int, speed: Optional[float] = None, memory_info: Optional[str] = None) -> None:
    """Helper per logging progresso batch con informazioni opzionali."""
    pct = (processed / total * 100) if total > 0 else 0
    speed_info = f" | {speed:.0f} rec/s" if speed else ""
    memory_info_str = f" | RAM: {memory_info}" if memory_info else ""
    logger_instance.info(f"[BATCH] Batch: {processed:,}/{total:,} ({pct:.1f}%){speed_info}{memory_info_str}")


def log_error_with_context(logger_instance: logging.Logger, error: Exception, context: str = "", operation: str = "") -> None:
    """Helper per logging errori con contesto."""
    context_str = f"[{context}] " if context else ""
    operation_str = f" durante {operation}" if operation else ""
    logger_instance.error(f"[ERROR] {context_str}Errore{operation_str}: {error}")


def log_resource_optimization(logger_instance: logging.Logger, 
                               cpu_cores: int = None, 
                               num_threads: int = None, 
                               total_memory_gb: float = None, 
                               usable_memory_gb: float = None, 
                               memory_buffer_ratio: float = None, 
                               num_workers: int = None, 
                               batch_size: int = None, 
                               max_chunk_size: int = None,
                               current_insert_batch: int = None) -> None:
    """Helper per logging configurazione risorse ottimizzate."""
    
    # Usa psutil per ottenere i valori se non forniti
    if cpu_cores is None:
        import multiprocessing
        cpu_cores = multiprocessing.cpu_count()
    
    if total_memory_gb is None:
        memory_info = psutil.virtual_memory()
        total_memory_gb = memory_info.total / (1024**3)
    
    if usable_memory_gb is None:
        memory_info = psutil.virtual_memory()
        buffer_ratio = memory_buffer_ratio or 0.2
        usable_memory_gb = (memory_info.total * (1 - buffer_ratio)) / (1024**3)
    
    logger_instance.info("[CONFIG] Configurazione risorse DINAMICHE ottimizzate:")
    logger_instance.info(f"   [CPU] CPU: {cpu_cores} core -> {num_threads or 'N/A'} thread attivi")
    logger_instance.info(f"   [RAM] RAM totale: {total_memory_gb:.1f}GB")
    logger_instance.info(f"   [RAM] RAM usabile: {usable_memory_gb:.1f}GB")
    logger_instance.info(f"   [PROC] Worker process: {num_workers or 'N/A'}")
    logger_instance.info(f"   [BATCH] Batch size principale: {batch_size or 'N/A':,}")
    
    current_ram = psutil.virtual_memory().available / (1024**3)
    logger_instance.info(f"   [INSERT] INSERT batch dinamico: {current_insert_batch or 'N/A':,} (RAM disponibile: {current_ram:.1f}GB)")
    logger_instance.info(f"   [CHUNK] Chunk size max: {max_chunk_size or 'N/A':,}") 