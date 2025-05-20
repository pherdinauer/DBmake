import sqlite3
import pandas as pd
from tabulate import tabulate
import os

def connect_db(db_path="database.db"):
    """Connette al database SQLite."""
    if not os.path.exists(db_path):
        print(f"❌ Database non trovato: {db_path}")
        return None
    return sqlite3.connect(db_path)

def show_tables(conn):
    """Mostra tutte le tabelle nel database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("\n📋 Tabelle disponibili:")
    for table in tables:
        print(f"- {table[0]}")

def show_table_info(conn, table_name):
    """Mostra la struttura di una tabella."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    print(f"\n📊 Struttura tabella {table_name}:")
    for col in columns:
        print(f"- {col[1]} ({col[2]})")

def search_cig(conn, cig):
    """Cerca un CIG specifico in tutte le tabelle."""
    # Cerca nella tabella principale
    df_cig = pd.read_sql_query(f"""
        SELECT * FROM cig WHERE cig LIKE '%{cig}%'
    """, conn)
    
    if not df_cig.empty:
        print("\n📌 Informazioni CIG:")
        print(tabulate(df_cig, headers='keys', tablefmt='psql', showindex=False))
        
        # Cerca nei bandi
        df_bandi = pd.read_sql_query(f"""
            SELECT * FROM bandi WHERE cig LIKE '%{cig}%'
        """, conn)
        if not df_bandi.empty:
            print("\n📋 Bandi associati:")
            print(tabulate(df_bandi, headers='keys', tablefmt='psql', showindex=False))
        
        # Cerca nelle aggiudicazioni
        df_aggiudicazioni = pd.read_sql_query(f"""
            SELECT * FROM aggiudicazioni WHERE cig LIKE '%{cig}%'
        """, conn)
        if not df_aggiudicazioni.empty:
            print("\n💰 Aggiudicazioni:")
            print(tabulate(df_aggiudicazioni, headers='keys', tablefmt='psql', showindex=False))
        
        # Cerca nei partecipanti
        df_partecipanti = pd.read_sql_query(f"""
            SELECT * FROM partecipanti WHERE cig LIKE '%{cig}%'
        """, conn)
        if not df_partecipanti.empty:
            print("\n👥 Partecipanti:")
            print(tabulate(df_partecipanti, headers='keys', tablefmt='psql', showindex=False))
        
        # Cerca nelle varianti
        df_varianti = pd.read_sql_query(f"""
            SELECT * FROM varianti WHERE cig LIKE '%{cig}%'
        """, conn)
        if not df_varianti.empty:
            print("\n📝 Varianti:")
            print(tabulate(df_varianti, headers='keys', tablefmt='psql', showindex=False))
    else:
        print(f"\n❌ Nessun CIG trovato: {cig}")

def show_stats(conn):
    """Mostra statistiche del database."""
    stats = {
        'CIG totali': pd.read_sql_query("SELECT COUNT(*) as count FROM cig", conn).iloc[0]['count'],
        'Bandi': pd.read_sql_query("SELECT COUNT(*) as count FROM bandi", conn).iloc[0]['count'],
        'Aggiudicazioni': pd.read_sql_query("SELECT COUNT(*) as count FROM aggiudicazioni", conn).iloc[0]['count'],
        'Partecipanti': pd.read_sql_query("SELECT COUNT(*) as count FROM partecipanti", conn).iloc[0]['count'],
        'Varianti': pd.read_sql_query("SELECT COUNT(*) as count FROM varianti", conn).iloc[0]['count']
    }
    
    print("\n📊 Statistiche Database:")
    for key, value in stats.items():
        print(f"- {key}: {value:,}")

def main():
    conn = connect_db()
    if not conn:
        return
    
    while True:
        print("\n🔍 Menu Consultazione Database:")
        print("1. Mostra tabelle")
        print("2. Mostra struttura tabella")
        print("3. Cerca CIG")
        print("4. Mostra statistiche")
        print("5. Esci")
        
        choice = input("\nScegli un'opzione (1-5): ")
        
        if choice == "1":
            show_tables(conn)
        
        elif choice == "2":
            table = input("Nome tabella: ")
            show_table_info(conn, table)
        
        elif choice == "3":
            cig = input("Inserisci CIG da cercare: ")
            search_cig(conn, cig)
        
        elif choice == "4":
            show_stats(conn)
        
        elif choice == "5":
            print("\n👋 Arrivederci!")
            break
        
        else:
            print("\n❌ Opzione non valida")
        
        input("\nPremi Invio per continuare...")

if __name__ == "__main__":
    main() 