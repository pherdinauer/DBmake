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
        self.logger.info(f"🔄 Inizio {self.operation_name}" + (f" ({context_str})" if context_str else ""))
        return self
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        elapsed = time.time() - (self.start_time or 0)
        if exc_type is None:
            self.logger.info(f"✅ Completato {self.operation_name} in {elapsed:.1f}s")
        else:
            self.logger.error(f"❌ Errore in {self.operation_name} dopo {elapsed:.1f}s: {exc_val}")


def log_memory_status(logger_instance: logging.Logger, context: str = "") -> None:
    """Helper per logging status memoria."""
    memory_info = psutil.virtual_memory()
    used_gb = memory_info.used / (1024**3)
    total_gb = memory_info.total / (1024**3)
    usage_pct = memory_info.percent
    available_gb = memory_info.available / (1024**3)
    
    prefix = f"[{context}] " if context else ""
    logger_instance.info(f"💻 {prefix}RAM: {used_gb:.1f}GB/{total_gb:.1f}GB ({usage_pct:.1f}%) | Disponibile: {available_gb:.1f}GB")


def log_performance_stats(logger_instance: logging.Logger, operation: str, count: int, elapsed_time: float, context: str = "") -> None:
    """Helper per logging statistiche performance."""
    speed = count / elapsed_time if elapsed_time > 0 else 0
    prefix = f"[{context}] " if context else ""
    logger_instance.info(f"📊 {prefix}{operation}: {count:,} elementi in {elapsed_time:.1f}s ({speed:.1f} el/s)")


def log_file_progress(logger_instance: logging.Logger, current: int, total: int, file_name: str = "", extra_info: str = "") -> None:
    """Helper per logging progresso file."""
    pct = (current / total * 100) if total > 0 else 0
    file_info = f" - {file_name}" if file_name else ""
    extra = f" | {extra_info}" if extra_info else ""
    logger_instance.info(f"📁 Progresso: {current}/{total} ({pct:.1f}%){file_info}{extra}")


def log_batch_progress(logger_instance: logging.Logger, processed: int, total: int, speed: Optional[float] = None, memory_info: Optional[str] = None) -> None:
    """Helper per logging progresso batch con informazioni opzionali."""
    pct = (processed / total * 100) if total > 0 else 0
    speed_info = f" | {speed:.0f} rec/s" if speed else ""
    memory_info_str = f" | RAM: {memory_info}" if memory_info else ""
    logger_instance.info(f"📦 Batch: {processed:,}/{total:,} ({pct:.1f}%){speed_info}{memory_info_str}")


def log_error_with_context(logger_instance: logging.Logger, error: Exception, context: str = "", operation: str = "") -> None:
    """Helper per logging errori con contesto."""
    context_str = f"[{context}] " if context else ""
    operation_str = f" durante {operation}" if operation else ""
    logger_instance.error(f"❌ {context_str}Errore{operation_str}: {error}")


def log_resource_optimization(logger_instance: logging.Logger) -> None:
    """Helper per logging configurazione risorse ottimizzate."""
    from ..import_json_mysql import (
        CPU_CORES, NUM_THREADS, TOTAL_MEMORY_GB, USABLE_MEMORY_GB, 
        MEMORY_BUFFER_RATIO, NUM_WORKERS, BATCH_SIZE, MAX_CHUNK_SIZE,
        calculate_dynamic_insert_batch_size
    )
    
    logger_instance.info("🚀 Configurazione risorse DINAMICHE ottimizzate:")
    logger_instance.info(f"   💻 CPU: {CPU_CORES} core → {NUM_THREADS} thread attivi ({(NUM_THREADS/CPU_CORES*100):.0f}% utilizzo)")
    logger_instance.info(f"   🖥️ RAM totale: {TOTAL_MEMORY_GB:.1f}GB")
    logger_instance.info(f"   🚀 RAM usabile: {USABLE_MEMORY_GB:.1f}GB (buffer {MEMORY_BUFFER_RATIO*100:.0f}%)")
    logger_instance.info(f"   🔥 Worker process: {NUM_WORKERS} (MONO-PROCESSO + thread aggressivi)")
    logger_instance.info(f"   📦 Batch size principale: {BATCH_SIZE:,}")
    
    current_insert_batch = calculate_dynamic_insert_batch_size()
    current_ram = psutil.virtual_memory().available / (1024**3)
    logger_instance.info(f"   ⚡ INSERT batch dinamico: {current_insert_batch:,} (RAM disponibile: {current_ram:.1f}GB)")
    logger_instance.info(f"   🎯 Chunk size max: {MAX_CHUNK_SIZE:,}") 