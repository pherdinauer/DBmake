#!/usr/bin/env python3
"""
MySQL Import Wrapper with InterfaceError handling
Wrapper robusto per gestire errori MySQLInterfaceError durante la connessione e creazione database
"""

import os
import sys
import logging
import mysql.connector
from mysql.connector import errorcode
import time
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Parametri di connessione da variabili d'ambiente
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_USER = os.environ.get('MYSQL_USER', 'Nando')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'DataBase2025!')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'anac_import3')

def safe_str_from_mysql_error(error):
    """
    Estrae in modo sicuro il messaggio di errore da un MySQLInterfaceError.
    
    Args:
        error: L'eccezione MySQL
        
    Returns:
        str: Messaggio di errore formattato in modo sicuro
    """
    try:
        # Prova ad accedere al messaggio come stringa
        return str(error)
    except:
        try:
            # Prova ad accedere agli argomenti dell'errore
            if hasattr(error, 'args') and error.args:
                return str(error.args[0])
        except:
            pass
        
        try:
            # Prova ad accedere ad errno e sqlstate
            errno = getattr(error, 'errno', 'N/A')
            sqlstate = getattr(error, 'sqlstate', 'N/A')
            error_type = type(error).__name__
            return f"MySQL error occurred (type: {error_type}, errno: {errno}, sqlstate: {sqlstate})"
        except:
            # Fallback finale
            return f"MySQL error occurred (type: {type(error).__name__})"

def test_mysql_connection():
    """Testa la connessione MySQL senza tentare di creare il database."""
    logger.info("🔍 Test connessione MySQL...")
    logger.info(f"   • Host: {MYSQL_HOST}")
    logger.info(f"   • User: {MYSQL_USER}")
    logger.info(f"   • Database target: {MYSQL_DATABASE}")
    
    try:
        # Test di connessione senza specificare database
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            autocommit=True
        )
        conn.close()
        logger.info("✅ Test di connessione MySQL riuscito!")
        return True
    except mysql.connector.Error as e:
        # Usa la funzione sicura per gestire l'errore
        safe_message = safe_str_from_mysql_error(e)
        logger.error(f"❌ Test di connessione fallito: {safe_message}")
        return False

def create_database_if_not_exists():
    """Crea il database se non esiste, gestendo InterfaceError in modo robusto."""
    logger.info(f"🔧 Verifica/creazione database '{MYSQL_DATABASE}'...")
    
    try:
        # Connessione senza database specificato
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            autocommit=True
        )
        
        cursor = conn.cursor()
        
        # Verifica se il database esiste
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        
        if MYSQL_DATABASE in databases:
            logger.info(f"✅ Database '{MYSQL_DATABASE}' già esiste")
        else:
            logger.info(f"🔨 Creazione database '{MYSQL_DATABASE}'...")
            cursor.execute(f"CREATE DATABASE `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"✅ Database '{MYSQL_DATABASE}' creato con successo")
        
        cursor.close()
        conn.close()
        return True
        
    except mysql.connector.InterfaceError as e:
        # Gestione specifica per InterfaceError
        safe_message = safe_str_from_mysql_error(e)
        logger.error(f"❌ InterfaceError durante creazione database: {safe_message}")
        return False
    except mysql.connector.Error as e:
        # Gestione per altri errori MySQL
        safe_message = safe_str_from_mysql_error(e)
        logger.error(f"❌ Errore MySQL durante creazione database: {safe_message}")
        return False
    except Exception as e:
        # Gestione per errori generici
        logger.error(f"❌ Errore generico durante creazione database: {e}")
        return False

def test_database_connection():
    """Testa la connessione al database specifico."""
    logger.info(f"🔍 Test connessione al database '{MYSQL_DATABASE}'...")
    
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            autocommit=True
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result and result[0] == 1:
            logger.info(f"✅ Connessione al database '{MYSQL_DATABASE}' riuscita!")
            return True
        else:
            logger.error(f"❌ Test query fallito")
            return False
            
    except mysql.connector.InterfaceError as e:
        safe_message = safe_str_from_mysql_error(e)
        logger.error(f"❌ InterfaceError durante test database: {safe_message}")
        return False
    except mysql.connector.Error as e:
        safe_message = safe_str_from_mysql_error(e)
        logger.error(f"❌ Errore MySQL durante test database: {safe_message}")
        return False
    except Exception as e:
        logger.error(f"❌ Errore generico durante test database: {e}")
        return False

def reset_processed_files():
    """Resetta la tabella processed_files per permettere re-import."""
    logger.info("🔧 Reset Cache Importazione ANAC")
    logger.info("=" * 50)
    
    try:
        # Connessione al database
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            autocommit=True
        )
        cursor = conn.cursor()
        
        # Controlla quanti file sono marcati come processati
        cursor.execute("SELECT COUNT(*) FROM processed_files WHERE status = 'completed'")
        completed_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processed_files WHERE status = 'failed'")
        failed_count = cursor.fetchone()[0]
        
        logger.info(f"📊 File attualmente marcati come processati:")
        logger.info(f"    ✅ Completati: {completed_count}")
        logger.info(f"    ❌ Falliti: {failed_count}")
        logger.info(f"    📝 Totale: {completed_count + failed_count}")
        
        if completed_count + failed_count == 0:
            logger.info("✅ Nessun file da resettare - tabella già vuota")
            return True
        
        # Opzioni di reset
        print("\n🔧 Opzioni di reset disponibili:")
        print("    1. Resetta SOLO i file falliti (recommended)")
        print("    2. Resetta TUTTI i file (re-import completo)")  
        print("    3. Mostra dettagli file falliti")
        print("    4. Esci senza modifiche")
        
        choice = input("\n👉 Scegli opzione (1-4): ").strip()
        
        if choice == "1":
            # Reset solo file falliti
            cursor.execute("DELETE FROM processed_files WHERE status = 'failed'")
            deleted = cursor.rowcount
            logger.info(f"✅ Rimossi {deleted} file falliti dalla lista")
            logger.info("🚀 Ora puoi rilanciare l'importazione per processare i file falliti")
            
        elif choice == "2":
            # Reset completo
            print("⚠️  Stai per resettare TUTTI i file processati!")
            print("   Questo farà ripartire l'importazione da zero per tutti i 647 file.")
            confirm = input("   Sei sicuro? Digita 'RESET' per confermare: ").strip()
            if confirm == 'RESET':
                cursor.execute("DELETE FROM processed_files")
                deleted = cursor.rowcount
                logger.info(f"✅ Rimossi TUTTI i {deleted} file dalla lista")
                logger.info("🚀 Ora puoi rilanciare l'importazione completa")
            else:
                logger.info("❌ Reset annullato - conferma non corretta")
                
        elif choice == "3":
            # Mostra dettagli file falliti
            cursor.execute("""
                SELECT file_name, processed_at, error_message 
                FROM processed_files 
                WHERE status = 'failed' 
                ORDER BY processed_at DESC 
                LIMIT 20
            """)
            failed_files = cursor.fetchall()
            
            if failed_files:
                logger.info(f"\n📋 Ultimi {len(failed_files)} file falliti:")
                for file_name, processed_at, error_msg in failed_files:
                    logger.info(f"    ❌ {file_name} ({processed_at})")
                    if error_msg:
                        logger.info(f"       Errore: {error_msg[:100]}...")
            else:
                logger.info("✅ Nessun file fallito trovato")
                
        elif choice == "4":
            logger.info("👋 Uscita senza modifiche")
            
        else:
            logger.error("❌ Opzione non valida")
            return False
            
        cursor.close()
        conn.close()
        return True
        
    except mysql.connector.Error as e:
        safe_message = safe_str_from_mysql_error(e)
        logger.error(f"❌ Errore MySQL durante reset: {safe_message}")
        return False
    except Exception as e:
        logger.error(f"❌ Errore durante reset: {e}")
        return False

def show_menu():
    """Mostra il menu principale con le opzioni disponibili."""
    print("\n" + "=" * 60)
    print("🔧 ANAC MySQL Import Tool")
    print("=" * 60)
    print("Scegli un'opzione:")
    print("  1. 🚀 Avvia importazione dati ANAC")
    print("  2. 🔧 Reset cache file processati")
    print("  3. 📊 Mostra stato database")
    print("  4. 🔍 Test connessione MySQL")
    print("  5. ❌ Esci")
    print("=" * 60)
    
    choice = input("👉 Scegli opzione (1-5): ").strip()
    return choice

def show_database_status():
    """Mostra lo stato del database e delle tabelle."""
    logger.info("📊 Verifica stato database...")
    
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            autocommit=True
        )
        cursor = conn.cursor()
        
        # Tabelle esistenti
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        logger.info(f"📋 Tabelle nel database '{MYSQL_DATABASE}': {len(tables)}")
        
        # Statistiche processed_files
        if 'processed_files' in tables:
            cursor.execute("SELECT status, COUNT(*) FROM processed_files GROUP BY status")
            status_counts = dict(cursor.fetchall())
            
            logger.info("📊 Stato file processati:")
            logger.info(f"    ✅ Completati: {status_counts.get('completed', 0)}")
            logger.info(f"    ❌ Falliti: {status_counts.get('failed', 0)}")
        
        # Tabelle dati per categoria
        data_tables = [t for t in tables if t.endswith('_data')]
        if data_tables:
            logger.info(f"📁 Tabelle dati categorie: {len(data_tables)}")
            
            total_records = 0
            for table in data_tables[:10]:  # Mostra solo le prime 10
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    total_records += count
                    logger.info(f"    • {table}: {count:,} record")
                except:
                    logger.info(f"    • {table}: [errore conteggio]")
            
            if len(data_tables) > 10:
                logger.info(f"    ... e altre {len(data_tables) - 10} tabelle")
                
            logger.info(f"📈 Record totali (prime 10 tabelle): {total_records:,}")
        
        cursor.close()
        conn.close()
        return True
        
    except mysql.connector.Error as e:
        safe_message = safe_str_from_mysql_error(e)
        logger.error(f"❌ Errore durante verifica stato: {safe_message}")
        return False
    except Exception as e:
        logger.error(f"❌ Errore durante verifica stato: {e}")
        return False

def run_import_script():
    """Esegue il vero script di import."""
    logger.info("🚀 Avvio script di importazione...")
    
    try:
        # Import del modulo principale
        from import_json_mysql import main as import_main
        
        # Esegue l'import
        import_main()
        logger.info("✅ Import completato con successo!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Errore durante l'importazione: {e}")
        return False

def main():
    """Funzione principale del wrapper con menu interattivo."""
    # Parsing argomenti da linea di comando
    parser = argparse.ArgumentParser(description='ANAC MySQL Import Tool')
    parser.add_argument('--menu', action='store_true', help='Mostra menu interattivo')
    parser.add_argument('--reset', action='store_true', help='Reset cache file processati')
    parser.add_argument('--status', action='store_true', help='Mostra stato database')
    parser.add_argument('--run', action='store_true', help='Avvia importazione diretta')
    args = parser.parse_args()
    
    logger.info("🔧 ANAC MySQL Import Tool")
    logger.info("=" * 50)
    
    # Test connessioni di base
    if not test_mysql_connection():
        logger.error("❌ Impossibile connettersi a MySQL. Verifica le credenziali.")
        sys.exit(1)
    
    if not create_database_if_not_exists():
        logger.error("❌ Impossibile creare/verificare il database.")
        sys.exit(1)
    
    if not test_database_connection():
        logger.error("❌ Impossibile connettersi al database target.")
        sys.exit(1)
    
    # Gestione argomenti da CLI
    if args.reset:
        if reset_processed_files():
            logger.info("🎉 Reset completato!")
        else:
            logger.error("❌ Reset fallito!")
        sys.exit(0)
    
    elif args.status:
        if show_database_status():
            logger.info("🎉 Verifica completata!")
        else:
            logger.error("❌ Verifica fallita!")
        sys.exit(0)
    
    elif args.run:
        if run_import_script():
            logger.info("🎉 Importazione completata!")
        else:
            logger.error("❌ Importazione fallita!")
        sys.exit(0)
    
    # Menu interattivo (default o --menu)
    elif args.menu or len(sys.argv) == 1:
        while True:
            choice = show_menu()
            
            if choice == "1":
                logger.info("🚀 Avvio importazione...")
                if run_import_script():
                    logger.info("🎉 Importazione completata con successo!")
                else:
                    logger.error("❌ Importazione fallita!")
                    
            elif choice == "2":
                if reset_processed_files():
                    logger.info("🎉 Reset completato!")
                else:
                    logger.error("❌ Reset fallito!")
                    
            elif choice == "3":
                if show_database_status():
                    logger.info("🎉 Verifica completata!")
                else:
                    logger.error("❌ Verifica fallita!")
                    
            elif choice == "4":
                logger.info("🔍 Test connessione già eseguito all'avvio - connessione OK!")
                
            elif choice == "5":
                logger.info("👋 Arrivederci!")
                break
                
            else:
                logger.error("❌ Opzione non valida! Scegli tra 1-5.")
    
    else:
        # Modalità legacy (senza argomenti)
        logger.info("🚀 Modalità legacy - importazione diretta")
        if run_import_script():
            logger.info("🎉 Processo completato con successo!")
        else:
            logger.error("❌ Importazione fallita!")
            sys.exit(1)

if __name__ == "__main__":
    main() 