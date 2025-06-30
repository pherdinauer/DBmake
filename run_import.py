#!/usr/bin/env python3
"""
🚀 WRAPPER CONVENIENTE per le Soluzioni Dinamiche Import JSON

Uso semplificato:
  python run_import.py                    # Auto-mode (raccomandato)
  python run_import.py --auto             # Selezione automatica intelligente
  python run_import.py --smart            # Categorizzazione intelligente
  python run_import.py --streaming        # Streaming incrementale per grandi volumi
  python run_import.py --standard         # Modalità standard con auto-discovery
  python run_import.py --test             # Solo test categorizzazione
  python run_import.py --test-solutions   # Test tutte le soluzioni
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path

def set_env_mode(mode):
    """Imposta la variabile d'ambiente per la modalità."""
    os.environ['IMPORT_MODE'] = mode
    print(f"🔧 [CONFIG] Modalità impostata: {mode.upper()}")

def run_import():
    """Esegue l'import con la modalità corrente."""
    print("🚀 [START] Avvio importazione...")
    
    try:
        # Usa subprocess per eseguire il modulo di import
        cmd = [sys.executable, '-m', 'src.import_json_mysql']
        result = subprocess.run(cmd, capture_output=False, text=True)
        
        if result.returncode == 0:
            print("✅ [SUCCESS] Importazione completata con successo!")
            return True
        else:
            print(f"❌ [ERROR] Importazione fallita con codice: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ [ERROR] Errore durante l'esecuzione: {e}")
        return False

def run_test_categorization():
    """Esegue solo il test di categorizzazione."""
    print("🧪 [TEST] Avvio test categorizzazione...")
    
    try:
        cmd = [sys.executable, '-m', 'src.import_json_mysql', '--test']
        result = subprocess.run(cmd, capture_output=False, text=True)
        
        if result.returncode == 0:
            print("✅ [TEST-SUCCESS] Test categorizzazione completato!")
            return True
        else:
            print(f"❌ [TEST-ERROR] Test fallito con codice: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST-ERROR] Errore durante il test: {e}")
        return False

def run_test_solutions():
    """Esegue il test delle soluzioni dinamiche."""
    print("🧪 [TEST-ALL] Avvio test delle 3 soluzioni dinamiche...")
    
    try:
        cmd = [sys.executable, 'test_solutions.py', '--test-all']
        result = subprocess.run(cmd, capture_output=False, text=True)
        
        if result.returncode == 0:
            print("✅ [TEST-ALL-SUCCESS] Test di tutte le soluzioni completato!")
            return True
        else:
            print(f"❌ [TEST-ALL-ERROR] Test fallito con codice: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST-ALL-ERROR] Errore durante il test: {e}")
        return False

def run_import_direct():
    """Esegue importazione diretta con SMART mode."""
    print("🚀 [IMPORT-DIRECT] Avvio importazione diretta con SMART mode...")
    set_env_mode('smart')
    return run_import()

def run_fix_empty_tables():
    """Fix specifico per problema tabelle vuote."""
    print("🔧 [FIX-EMPTY] Fix specifico per tabelle vuote...")
    print("📋 [FIX-EMPTY] Questo risolve il problema usando SMART categorization")
    
    # Prima testa la categorizzazione
    print("🧪 [FIX-EMPTY] Step 1: Test categorizzazione...")
    if not run_test_categorization():
        print("❌ [FIX-EMPTY] Test categorizzazione fallito, risolvi prima questo")
        return False
    
    # Poi usa SMART mode per l'import
    print("🧠 [FIX-EMPTY] Step 2: Importazione con SMART mode...")
    set_env_mode('smart')
    return run_import()

def show_logs():
    """Mostra gli ultimi log di importazione."""
    print("📄 [LOGS] Ricerca ultimi log di importazione...")
    
    logs_dir = Path('./logs')
    if not logs_dir.exists():
        print("⚠️  [LOGS] Directory logs/ non trovata")
        return False
    
    # Trova ultimi log di import
    import_logs = sorted(logs_dir.glob('import_*.log'), key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not import_logs:
        print("⚠️  [LOGS] Nessun log di importazione trovato")
        return False
    
    latest_log = import_logs[0]
    print(f"📄 [LOGS] Ultimo log: {latest_log.name}")
    
    try:
        with open(latest_log, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        # Mostra ultime 50 righe
        print("📄 [LOGS] Ultime 50 righe:")
        print("-" * 70)
        for line in lines[-50:]:
            print(line.rstrip())
        print("-" * 70)
        
        return True
        
    except Exception as e:
        print(f"❌ [LOGS] Errore lettura log: {e}")
        return False

def print_status_info():
    """Stampa informazioni sullo stato corrente."""
    print("📊 [STATUS] Informazioni correnti:")
    
    # Path JSON
    json_paths = [
        './database/JSON',
        'database/JSON', 
        '/database/JSON'
    ]
    
    for path in json_paths:
        if Path(path).exists():
            file_count = len(list(Path(path).rglob("*.json")))
            size_mb = sum(f.stat().st_size for f in Path(path).rglob("*.json")) / (1024*1024)
            print(f"  📁 Path JSON trovato: {path}")
            print(f"     - File JSON: {file_count}")
            print(f"     - Dimensione totale: {size_mb:.1f} MB")
            break
    else:
        print("  ⚠️  Nessun path JSON trovato nelle posizioni standard")
    
    # Variabili d'ambiente
    import_mode = os.environ.get('IMPORT_MODE', 'auto')
    mysql_db = os.environ.get('MYSQL_DATABASE', 'anac_import3')
    mysql_host = os.environ.get('MYSQL_HOST', 'localhost')
    
    print(f"  🔧 Modalità import: {import_mode}")
    print(f"  🗄️  Database MySQL: {mysql_db}")
    print(f"  🏠 Host MySQL: {mysql_host}")

def main():
    parser = argparse.ArgumentParser(
        description='🚀 CLI UNIFICATO per Import JSON Dinamico ANAC',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🎯 COMANDI PRINCIPALI:
  python run_import.py                    # SMART mode (RACCOMANDATO per la tua struttura)
  python run_import.py --run              # Importazione diretta con SMART mode
  python run_import.py --test             # Test categorizzazione (SEMPRE prima!)
  python run_import.py --fix-empty        # Fix specifico per tabelle vuote
  
🔧 MODALITÀ AVANZATE:
  python run_import.py --auto             # Selezione automatica basata su dataset
  python run_import.py --streaming        # Per dataset enormi (>10GB)
  python run_import.py --standard         # Modalità base con pattern classici

📊 UTILITÀ:
  python run_import.py --status           # Info stato e configurazione
  python run_import.py --test-all         # Test tutte le soluzioni
  python run_import.py --logs             # Mostra ultimi log

💡 RACCOMANDAZIONE:
  Per la tua struttura con timestamp (20240201-categoria_json), 
  usa SMART mode che analizza il contenuto dei file oltre ai nomi.
        """
    )
    
    # COMANDI PRINCIPALI (mutualmente esclusivi)
    main_group = parser.add_mutually_exclusive_group()
    main_group.add_argument('--run', action='store_true',
                           help='🚀 IMPORTAZIONE DIRETTA con SMART mode (raccomandato per te)')
    main_group.add_argument('--test', action='store_true',
                           help='🧪 TEST categorizzazione (fai SEMPRE prima!)')
    main_group.add_argument('--fix-empty', action='store_true',
                           help='🔧 FIX SPECIFICO per tabelle vuote con SMART mode')
    
    # MODALITÀ AVANZATE
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--auto', action='store_true',
                           help='🤖 Auto-detect modalità in base a dimensione dataset')
    mode_group.add_argument('--smart', action='store_true',
                           help='🧠 SMART mode - analisi contenuto (MIGLIORE per te)')
    mode_group.add_argument('--streaming', action='store_true',
                           help='🌊 Streaming per dataset enormi (>10GB)')
    mode_group.add_argument('--standard', action='store_true',
                           help='📋 Modalità classica con pattern base')
    
    # UTILITÀ
    util_group = parser.add_mutually_exclusive_group()
    util_group.add_argument('--status', action='store_true',
                           help='📊 Info stato e configurazione sistema')
    util_group.add_argument('--test-all', action='store_true',
                           help='🧪 Test di tutte le 3 soluzioni dinamiche')
    util_group.add_argument('--logs', action='store_true',
                           help='📄 Mostra ultimi log di importazione')
    
    # OPZIONI GLOBALI
    parser.add_argument('--database', type=str,
                       help='🗄️  Database MySQL personalizzato')
    parser.add_argument('--path', type=str,
                       help='📁 Path JSON personalizzato')
    parser.add_argument('--force', action='store_true',
                       help='⚡ Salta conferme (per automazione)')
    
    args = parser.parse_args()
    
    # Banner di benvenuto
    print("=" * 70)
    print("🚀 IMPORTATORE JSON DINAMICO con 3 SOLUZIONI AVANZATE")
    print("=" * 70)
    
    # Gestione opzioni globali
    if args.database:
        os.environ['MYSQL_DATABASE'] = args.database
        print(f"🗄️  [CONFIG] Database personalizzato: {args.database}")
    
    if args.path:
        os.environ['ANAC_BASE_PATH'] = args.path
        print(f"📁 [CONFIG] Path JSON personalizzato: {args.path}")
    
    # COMANDI PRINCIPALI
    if args.run:
        return run_import_direct()
    
    if args.test:
        return run_test_categorization()
    
    if args.fix_empty:
        return run_fix_empty_tables()
    
    # UTILITÀ
    if args.status:
        print_status_info()
        return True
    
    if args.test_all:
        return run_test_solutions()
    
    if args.logs:
        return show_logs()
    
    # MODALITÀ AVANZATE o DEFAULT SMART
    if args.smart:
        set_env_mode('smart')
    elif args.streaming:
        set_env_mode('streaming')  
    elif args.standard:
        set_env_mode('standard')
    elif args.auto:
        set_env_mode('auto')
    else:
        # DEFAULT: SMART mode (migliore per la struttura dell'utente)
        set_env_mode('smart')
        print("💡 [DEFAULT] Nessuna modalità specificata, uso SMART-MODE (migliore per la tua struttura)")
    
    # Mostra info pre-import
    print_status_info()
    
    # Conferma utente (saltabile con --force)
    if not args.force:
        try:
            confirm = input("\n❓ Continuare con l'importazione? [S/n]: ").strip().lower()
            if confirm and confirm not in ['s', 'si', 'sì', 'y', 'yes']:
                print("❌ [CANCELLED] Importazione annullata dall'utente")
                return False
        except KeyboardInterrupt:
            print("\n❌ [CANCELLED] Importazione annullata dall'utente")
            return False
    else:
        print("⚡ [FORCE] Modalità automatica attivata, salto conferma")
    
    # Esegui import
    print("\n" + "=" * 70)
    success = run_import()
    print("=" * 70)
    
    if success:
        print("🎉 [COMPLETE] Importazione completata con successo!")
        print("💡 [INFO] Controlla i log in logs/ per dettagli")
    else:
        print("💔 [FAILED] Importazione fallita")
        print("🆘 [HELP] Prova:")
        print("  1. python run_import.py --test  # Testa categorizzazione")
        print("  2. python run_import.py --streaming  # Modalità più robusta")
        print("  3. Controlla logs/ per errori dettagliati")
    
    return success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n❌ [INTERRUPTED] Operazione interrotta dall'utente")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 [FATAL] Errore fatale: {e}")
        sys.exit(1) 