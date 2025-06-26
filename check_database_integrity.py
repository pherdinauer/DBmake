#!/usr/bin/env python3
"""
Sistema di Verifica dell'Integrità del Database
Verifica la consistenza dei dati direttamente dalle tabelle MySQL
"""

import sys
import os
import argparse
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from pathlib import Path
import json

def check_database_integrity(db_config, verbose=False):
    """
    Verifica l'integrità dei dati nel database
    """
    
    print("=" * 70)
    print("SISTEMA VERIFICA INTEGRITA DATABASE MYSQL")
    print("Controllo consistenza dati nelle tabelle")
    print("=" * 70)
    print(f"Host: {db_config['host']}")
    print(f"Database: {db_config['database']}")
    print(f"Modalità verbosa: {'Attiva' if verbose else 'Standard'}")
    print(f"Avvio: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 70)
    print("")
    
    try:
        # Connessione al database
        print("FASE 1: Connessione al database...")
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        print("SUCCESS: Connessione stabilita")
        print("")
        
        # Fase 2: Scoperta tabelle
        print("FASE 2: Scansione tabelle nel database...")
        cursor.execute(f"SHOW TABLES FROM {db_config['database']}")
        tables = [table[0] for table in cursor.fetchall()]
        
        # Filtra solo le tabelle dati (non di sistema)
        data_tables = [t for t in tables if '_data' in t or 'smartcig' in t]
        
        print(f"Tabelle totali trovate: {len(tables)}")
        print(f"Tabelle dati identificate: {len(data_tables)}")
        print("")
        
        if verbose:
            print("Tabelle dati trovate:")
            for table in data_tables:
                print(f"  • {table}")
            print("")
        
        # Fase 3: Conteggio record per tabella
        print("FASE 3: Conteggio record per tabella...")
        print("-" * 50)
        
        table_stats = {}
        total_records = 0
        
        for table in data_tables:
            try:
                # Conta record
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # Ottieni informazioni sulla tabella
                cursor.execute(f"SHOW CREATE TABLE {table}")
                create_table = cursor.fetchone()[1]
                
                # Ottieni dimensione tabella
                cursor.execute(f"""
                    SELECT 
                        ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'DB Size in MB' 
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                """, (db_config['database'], table))
                
                size_result = cursor.fetchone()
                size_mb = size_result[0] if size_result and size_result[0] else 0.0
                
                table_stats[table] = {
                    'records': count,
                    'size_mb': float(size_mb) if size_mb else 0.0
                }
                
                total_records += count
                
                print(f"{table:<35} | {count:>8,} record | {size_mb:>6.1f} MB")
                
                if verbose and count > 0:
                    # Mostra un record di esempio
                    cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                    columns = [desc[0] for desc in cursor.description]
                    sample_record = cursor.fetchone()
                    print(f"    Colonne ({len(columns)}): {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
                    if sample_record:
                        print(f"    Esempio: {str(sample_record)[:80]}{'...' if len(str(sample_record)) > 80 else ''}")
                    print("")
                
            except Error as e:
                print(f"{table:<35} | ERROR: {e}")
                table_stats[table] = {'records': 0, 'size_mb': 0.0, 'error': str(e)}
        
        print("-" * 50)
        print(f"{'TOTALE':<35} | {total_records:>8,} record")
        print("")
        
        # Fase 4: Verifica consistenza dati
        print("FASE 4: Verifica consistenza e integrità...")
        print("")
        
        issues_found = []
        
        # Verifica 1: Tabelle vuote
        empty_tables = [table for table, stats in table_stats.items() 
                       if stats.get('records', 0) == 0 and 'error' not in stats]
        
        if empty_tables:
            print("ATTENZIONE: Tabelle vuote trovate:")
            for table in empty_tables:
                print(f"  • {table}")
                issues_found.append(f"Tabella vuota: {table}")
            print("")
        
        # Verifica 2: Tabelle con errori
        error_tables = [table for table, stats in table_stats.items() if 'error' in stats]
        
        if error_tables:
            print("ERRORE: Tabelle con problemi di accesso:")
            for table in error_tables:
                print(f"  • {table}: {table_stats[table]['error']}")
                issues_found.append(f"Errore accesso tabella: {table}")
            print("")
        
        # Verifica 3: Distribuzione record per tipologia
        print("ANALISI DISTRIBUZIONE DATI:")
        
        # Raggruppa per tipologia
        type_groups = {}
        for table, stats in table_stats.items():
            if 'error' in stats:
                continue
                
            if 'smartcig' in table:
                type_name = 'SmartCIG'
            elif 'aggiudicatari' in table:
                type_name = 'Aggiudicatari'
            elif 'bandi' in table:
                type_name = 'Bandi'
            elif 'partecipanti' in table:
                type_name = 'Partecipanti'
            else:
                type_name = 'Altri'
            
            if type_name not in type_groups:
                type_groups[type_name] = {'tables': 0, 'records': 0, 'size_mb': 0.0}
            
            type_groups[type_name]['tables'] += 1
            type_groups[type_name]['records'] += stats['records']
            type_groups[type_name]['size_mb'] += stats['size_mb']
        
        for type_name, stats in type_groups.items():
            print(f"{type_name:<15} | {stats['tables']:>2} tabelle | {stats['records']:>8,} record | {stats['size_mb']:>6.1f} MB")
        
        print("")
        
        # Fase 5: Verifica specifica per chiavi primarie e duplicati
        print("FASE 5: Verifica qualità dati...")
        
        for table in data_tables[:5]:  # Verifica solo le prime 5 tabelle per velocità
            if table_stats[table].get('records', 0) == 0:
                continue
                
            try:
                # Verifica se ha una colonna ID o simile
                cursor.execute(f"DESCRIBE {table}")
                columns = [row[0] for row in cursor.fetchall()]
                
                # Cerca colonne chiave
                id_columns = [col for col in columns if col.lower() in ['id', 'cig', 'uuid', 'codice']]
                
                if id_columns:
                    id_col = id_columns[0]
                    
                    # Verifica duplicati
                    cursor.execute(f"""
                        SELECT COUNT(*) as total, COUNT(DISTINCT {id_col}) as unique_count 
                        FROM {table} 
                        WHERE {id_col} IS NOT NULL
                    """)
                    
                    total, unique = cursor.fetchone()
                    
                    if total != unique:
                        duplicates = total - unique
                        print(f"  ATTENZIONE: {table} ha {duplicates} duplicati su colonna {id_col}")
                        issues_found.append(f"Duplicati in {table}: {duplicates}")
                    elif verbose:
                        print(f"  OK: {table} - nessun duplicato su {id_col}")
                
            except Error as e:
                if verbose:
                    print(f"  Errore verifica {table}: {e}")
        
        print("")
        
        # Risultato finale
        print("=" * 70)
        print("RISULTATO CONTROLLO INTEGRITA DATABASE")
        print("=" * 70)
        
        if not issues_found:
            print("SUCCESS: INTEGRITA DATABASE CONFERMATA!")
            print("✓ Tutte le tabelle sono accessibili")
            print("✓ Nessun problema critico rilevato")
            print(f"✓ {total_records:,} record totali verificati")
            print(f"✓ {len(data_tables)} tabelle dati controllate")
        else:
            print("ATTENZIONE: Problemi rilevati durante la verifica")
            print(f"Problemi trovati: {len(issues_found)}")
            for issue in issues_found:
                print(f"  • {issue}")
        
        # Salva report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"logs/database_integrity_report_{timestamp}.json"
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'database': db_config['database'],
            'total_tables': len(data_tables),
            'total_records': total_records,
            'table_stats': table_stats,
            'type_distribution': type_groups,
            'issues_found': issues_found,
            'integrity_status': 'PASS' if not issues_found else 'ISSUES'
        }
        
        # Assicurati che la directory logs esista
        Path("logs").mkdir(exist_ok=True)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\nReport dettagliato salvato: {report_file}")
        print("=" * 70)
        
        cursor.close()
        connection.close()
        
        return 0 if not issues_found else 1
        
    except Error as e:
        print(f"ERRORE DATABASE: {e}")
        return 2
    except Exception as e:
        print(f"ERRORE: {e}")
        return 2

def main():
    """Funzione principale"""
    parser = argparse.ArgumentParser(
        description='Verifica integrità database MySQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo:
  python check_database_integrity.py                                    # Database di default
  python check_database_integrity.py --database anac_produzione        # Database personalizzato
  python check_database_integrity.py --database anac_test --verbose     # Con output verboso
        """
    )
    
    parser.add_argument(
        '--database',
        type=str,
        help='Nome del database da verificare (default: da variabile ambiente o anac_import3)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='Host del database MySQL (default: localhost)'
    )
    
    parser.add_argument(
        '--user',
        type=str,
        default='Nando',
        help='Username del database MySQL (default: Nando)'
    )
    
    parser.add_argument(
        '--password',
        type=str,
        help='Password del database MySQL (default: da variabile ambiente)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Output verboso con dettagli aggiuntivi'
    )
    
    args = parser.parse_args()
    
    # Configura il database
    db_config = {
        'host': args.host or os.getenv('MYSQL_HOST', 'localhost'),
        'user': args.user or os.getenv('MYSQL_USER', 'Nando'),
        'password': args.password or os.getenv('MYSQL_PASSWORD', ''),
        'database': args.database or os.getenv('MYSQL_DATABASE', 'anac_import3'),
        'charset': 'utf8mb4',
        'collation': 'utf8mb4_unicode_ci'
    }
    
    return check_database_integrity(db_config, args.verbose)

if __name__ == "__main__":
    exit(main()) 