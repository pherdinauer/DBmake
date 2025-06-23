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

def run_import_script(custom_database=None):
    """Esegue il vero script di import."""
    logger.info("🚀 Avvio script di importazione...")
    
    try:
        # Configura il database personalizzato se specificato
        if custom_database:
            logger.info(f"🔧 Configurazione database personalizzato: {custom_database}")
            os.environ['MYSQL_DATABASE'] = custom_database
            global MYSQL_DATABASE
            MYSQL_DATABASE = custom_database
        
        # Import del modulo principale aggiornato
        try:
            # Prova l'import relativo (quando eseguito come modulo)
            from .import_json_mysql import main as import_main
        except ImportError:
            # Fallback su import assoluto 
            from src.import_json_mysql import main as import_main
        
        # Esegue l'import
        import_main()
        logger.info("✅ Import completato con successo!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Errore durante l'importazione: {e}")
        return False

def get_database_choice():
    """Chiede all'utente di scegliere il database da utilizzare."""
    print("\n" + "="*70)
    print("🔧 CONFIGURAZIONE DATABASE")
    print("="*70)
    print("1) 📋 Usa database di default (anac_import3)")
    print("2) 🆕 Crea nuovo database personalizzato")
    print("3) 📁 Usa database esistente (specifica nome)")
    print("="*70)
    
    while True:
        try:
            choice = input("Scegli opzione (1-3): ").strip()
            
            if choice == "1":
                return "anac_import3"
            
            elif choice == "2":
                print("\n🆕 CREAZIONE NUOVO DATABASE")
                print("-" * 40)
                while True:
                    db_name = input("Inserisci nome del nuovo database: ").strip()
                    if db_name:
                        # Valida il nome del database
                        if db_name.replace('_', '').replace('-', '').isalnum():
                            confirm = input(f"Confermi creazione database '{db_name}'? (s/n): ").strip().lower()
                            if confirm in ['s', 'si', 'y', 'yes']:
                                print(f"✅ Database '{db_name}' sarà creato")
                                return db_name
                            else:
                                print("❌ Operazione annullata")
                                continue
                        else:
                            print("❌ Nome database non valido. Usa solo lettere, numeri, '_' e '-'")
                    else:
                        print("❌ Nome database non può essere vuoto")
            
            elif choice == "3":
                print("\n📁 USO DATABASE ESISTENTE")
                print("-" * 40)
                while True:
                    db_name = input("Inserisci nome database esistente: ").strip()
                    if db_name:
                        return db_name
                    else:
                        print("❌ Nome database non può essere vuoto")
            
            else:
                print("❌ Opzione non valida. Scegli 1, 2 o 3.")
                
        except KeyboardInterrupt:
            print("\n❌ Operazione interrotta dall'utente")
            sys.exit(1)
        except EOFError:
            print("\n❌ Input terminato")
            sys.exit(1)

def main():
    """Funzione principale del wrapper."""
    import argparse
    
    # Parse degli argomenti da riga di comando
    parser = argparse.ArgumentParser(description='MySQL Import Wrapper con gestione robusta InterfaceError')
    parser.add_argument('--database', type=str, help='Nome del database personalizzato da utilizzare')
    parser.add_argument('--interactive', '-i', action='store_true', help='Modalità interattiva per scegliere il database')
    args = parser.parse_args()
    
    # Aggiorna il database se specificato
    global MYSQL_DATABASE
    custom_database = args.database
    
    # Se modalità interattiva, chiedi all'utente
    if args.interactive and not custom_database:
        custom_database = get_database_choice()
    
    if custom_database:
        MYSQL_DATABASE = custom_database
        os.environ['MYSQL_DATABASE'] = custom_database
        logger.info(f"🔧 Utilizzo database personalizzato: {custom_database}")
    
    logger.info("🔧 MySQL Import Wrapper - Gestione robusta InterfaceError")
    logger.info("=" * 70)
    
    # Step 1: Test connessione base
    if not test_mysql_connection():
        logger.error("❌ Impossibile connettersi a MySQL. Verifica le credenziali.")
        sys.exit(1)
    
    # Step 2: Crea database se necessario
    if not create_database_if_not_exists():
        logger.error("❌ Impossibile creare/verificare il database.")
        sys.exit(1)
    
    # Step 3: Test connessione al database
    if not test_database_connection():
        logger.error("❌ Impossibile connettersi al database target.")
        sys.exit(1)
    
    # Step 4: Esegui import
    if not run_import_script(custom_database):
        logger.error("❌ Importazione fallita.")
        sys.exit(1)
    
    logger.info("🎉 Processo completato con successo!")

if __name__ == "__main__":
    main() 