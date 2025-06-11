#!/usr/bin/env python3
"""
Dynamic Schema Manager - Sistema proattivo per gestione schema MySQL
"""

import logging
import mysql.connector
import re
from typing import Dict
import threading

schema_logger = logging.getLogger('dynamic_schema')

class DynamicSchemaManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.created_tables = set()
        self.table_columns = {}
        self.schema_lock = threading.Lock()
    
    def ensure_table_exists(self, category: str, sample_record: dict) -> str:
        """Assicura che la tabella per la categoria esista."""
        table_name = f"{category}_data"
        
        with self.schema_lock:
            if table_name in self.created_tables:
                return table_name
            
            if not self._check_table_exists(table_name):
                schema_logger.info(f"🏗️  [CREATE] Creazione dinamica tabella: {table_name}")
                self._create_table_from_sample(table_name, sample_record)
            
            self._load_table_structure(table_name)
            self.created_tables.add(table_name)
            return table_name
    
    def ensure_columns_exist(self, table_name: str, record: dict):
        """Assicura che tutte le colonne necessarie esistano."""
        with self.schema_lock:
            current_columns = self.table_columns.get(table_name, set())
            record_fields = set(record.keys())
            system_fields = {'cig', 'id', 'created_at', 'source_file', 'batch_id'}
            missing_columns = record_fields - current_columns - system_fields
            
            if missing_columns:
                schema_logger.info(f"🔧 [ALTER] Aggiunta {len(missing_columns)} colonne a {table_name}")
                for column_name in missing_columns:
                    mysql_type = self._detect_mysql_type(column_name, record.get(column_name))
                    self._add_column_to_table(table_name, column_name, mysql_type)
                    current_columns.add(column_name)
                self.table_columns[table_name] = current_columns
    
    def _check_table_exists(self, table_name: str) -> bool:
        try:
            result = self.db_manager.execute_with_retry(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
                params=(table_name,), fetch=True
            )
            return result[0][0] > 0
        except:
            return False
    
    def _create_table_from_sample(self, table_name: str, sample_record: dict):
        columns_def = []
        for field_name, field_value in sample_record.items():
            if field_name.lower() == 'cig':
                continue
            mysql_type = self._detect_mysql_type(field_name, field_value)
            sanitized_name = self._sanitize_column_name(field_name)
            columns_def.append(f"{sanitized_name} {mysql_type}")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            cig VARCHAR(64) NOT NULL,
            {', '.join(columns_def)},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file VARCHAR(255),
            batch_id VARCHAR(64),
            INDEX idx_cig (cig)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        self.db_manager.execute_with_retry(create_sql)
        schema_logger.info(f"✅ [CREATED] Tabella {table_name} con {len(columns_def)} colonne")
    
    def _add_column_to_table(self, table_name: str, column_name: str, mysql_type: str):
        sanitized_name = self._sanitize_column_name(column_name)
        if sanitized_name.lower() in ['id', 'cig', 'created_at', 'source_file', 'batch_id']:
            return
        
        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {sanitized_name} {mysql_type}"
        try:
            self.db_manager.execute_with_retry(alter_sql)
            schema_logger.info(f"➕ [COLUMN] {sanitized_name} ({mysql_type}) -> {table_name}")
        except mysql.connector.Error as e:
            if "Duplicate column name" not in str(e):
                schema_logger.error(f"❌ Errore aggiunta colonna: {e}")
    
    def _detect_mysql_type(self, field_name: str, field_value) -> str:
        if field_value is None:
            return "TEXT"
        
        if isinstance(field_value, bool):
            return "CHAR(1) DEFAULT 'N'"
        elif isinstance(field_value, int):
            return "INT" if -2147483648 <= field_value <= 2147483647 else "BIGINT"
        elif isinstance(field_value, float):
            return "DECIMAL(15,4)"
        elif isinstance(field_value, (list, dict)):
            return "JSON"
        
        if isinstance(field_value, str):
            value_length = len(str(field_value))
            field_lower = field_name.lower()
            
            if any(flag in field_lower for flag in ['flag_', '_flag']):
                return "CHAR(1) DEFAULT 'N'"
            elif 'codice_fiscale' in field_lower:
                return "VARCHAR(16)"
            elif 'partita_iva' in field_lower:
                return "VARCHAR(11)"
            elif 'email' in field_lower:
                return "VARCHAR(255)"
            elif any(date_field in field_lower for date_field in ['data_', 'date_']):
                return "DATE"
            elif value_length <= 50:
                return f"VARCHAR({max(value_length * 2, 50)})"
            elif value_length <= 255:
                return "VARCHAR(255)"
            else:
                return "TEXT"
        
        return "TEXT"
    
    def _sanitize_column_name(self, field_name: str) -> str:
        sanitized = re.sub(r'[^\w]', '_', field_name.lower())
        sanitized = re.sub(r'_+', '_', sanitized).strip('_')
        
        if sanitized and sanitized[0].isdigit():
            sanitized = f"field_{sanitized}"
        
        if len(sanitized) > 64:
            sanitized = sanitized[:61] + "_tr"
        
        return sanitized or "unknown_field"
    
    def _load_table_structure(self, table_name: str):
        try:
            result = self.db_manager.execute_with_retry(f"DESCRIBE {table_name}", fetch=True)
            columns = {row[0] for row in result}
            self.table_columns[table_name] = columns
        except Exception as e:
            schema_logger.warning(f"⚠️  Errore caricamento struttura {table_name}: {e}")
            self.table_columns[table_name] = set() 