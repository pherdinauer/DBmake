#!/usr/bin/env python3
"""
Script per eseguire la verifica dell'integrità dei dati
Uso: python check_data_integrity.py [--database DATABASE_NAME]
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime

# Aggiungi il percorso src al path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_integrity_checker import DataIntegrityChecker, main

def parse_arguments():
    """Parse degli argomenti da riga di comando"""
    parser = argparse.ArgumentParser(
        description='Verifica integrità dati JSON vs Database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo:
  python check_data_integrity.py                                    # Database di default
  python check_data_integrity.py --database anac_produzione        # Database personalizzato
  python check_data_integrity.py --database anac_test_2025         # Database di test
        """
    )
    
    parser.add_argument(
        '--database',
        type=str,
        help='Nome del database da utilizzare per la verifica (default: da variabile ambiente o anac_import3)'
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
    
    return parser.parse_args()

def run_integrity_check():
    """Esegue la verifica dell'integrità con parametri personalizzati"""
    args = parse_arguments()
    
    # Configura il database
    db_config = {
        'host': args.host or os.getenv('MYSQL_HOST', 'localhost'),
        'user': args.user or os.getenv('MYSQL_USER', 'Nando'),
        'password': args.password or os.getenv('MYSQL_PASSWORD', ''),
        'database': args.database or os.getenv('MYSQL_DATABASE', 'anac_import3'),
        'charset': 'utf8mb4',
        'collation': 'utf8mb4_unicode_ci'
    }
    
    print("🔧 CONFIGURAZIONE VERIFICA INTEGRITÀ DATI")
    print("="*60)
    print(f"🌐 Host MySQL: {db_config['host']}")
    print(f"👤 Utente: {db_config['user']}")
    print(f"🗄️ Database: {db_config['database']}")
    print(f"🔍 Modalità verbosa: {'Attiva' if args.verbose else 'Standard'}")
    print(f"⏰ Avvio: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*60)
    print("🚀 Inizializzazione controllo di sicurezza dei dati...")
    print("")
    
    # Configura il logging se verbose
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Assicurati che le directory dei log esistano
        Path("logs").mkdir(exist_ok=True)
        Path("logs/missing_data").mkdir(exist_ok=True)
        
        # Crea checker con configurazione personalizzata
        checker = DataIntegrityChecker(db_config)
        
        # Esegui verifica completa
        print("\n🚀 Avvio verifica integrità...")
        global_report = checker.run_full_integrity_check()
        
        # Genera log dettagliato
        checker.generate_detailed_log(global_report)
        
        print("\n📋 RISULTATI FINALI:")
        print("-" * 30)
        
        if global_report.total_missing_records == 0:
            print("🎉 SUCCESSO: Tutti i dati sono stati importati correttamente!")
            print(f"✅ {global_report.total_source_records:,} record verificati")
            return 0
        else:
            print(f"⚠️ ATTENZIONE: {global_report.total_missing_records:,} record mancanti")
            print(f"📊 Tasso successo: {global_report.global_success_rate:.2f}%")
            print(f"📁 File con problemi: {len(global_report.files_with_issues)}")
            
            if global_report.files_with_issues:
                print("\n🔍 File con problemi:")
                for filename in global_report.files_with_issues[:10]:  # Max 10
                    print(f"   • {filename}")
                if len(global_report.files_with_issues) > 10:
                    print(f"   ... e altri {len(global_report.files_with_issues) - 10} file")
            
            print(f"\n📄 Report dettagliati salvati in: logs/")
            print(f"📄 Dati mancanti salvati in: logs/missing_data/")
            
            return 1
            
    except Exception as e:
        print(f"💥 Errore durante la verifica: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 2

if __name__ == "__main__":
    exit(run_integrity_check()) 