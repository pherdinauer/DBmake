import sqlite3
import pandas as pd
from tabulate import tabulate
import os
import sys
from datetime import datetime

def clear_screen():
    """Pulisce lo schermo del terminale."""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    """Stampa l'intestazione del programma."""
    print("""
╔════════════════════════════════════════════════════════════════════════════╗
║                         CIG Database Query Tool                            ║
╚════════════════════════════════════════════════════════════════════════════╝
""")

def get_db_stats(conn):
    """Ottiene statistiche sul database."""
    stats = {}
    tables = ['cig', 'bandi', 'aggiudicazioni', 'partecipanti', 'varianti']
    
    for table in tables:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        stats[table] = cursor.fetchone()[0]
    
    return stats

def search_cig(conn, search_term, limit=10):
    """Cerca CIG che corrispondono al termine di ricerca."""
    query = """
    SELECT c.cig, c.oggetto, c.importo, c.data_pubblicazione,
           b.tipo_bando,
           a.importo_aggiudicazione,
           COUNT(DISTINCT p.id) as num_partecipanti,
           COUNT(DISTINCT v.id) as num_varianti
    FROM cig c
    LEFT JOIN bandi b ON c.cig = b.cig
    LEFT JOIN aggiudicazioni a ON c.cig = a.cig
    LEFT JOIN partecipanti p ON c.cig = p.cig
    LEFT JOIN varianti v ON c.cig = v.cig
    WHERE c.cig LIKE ? OR c.oggetto LIKE ?
    GROUP BY c.cig
    LIMIT ?
    """
    search_pattern = f"%{search_term}%"
    return pd.read_sql_query(query, conn, params=(search_pattern, search_pattern, limit))

def get_cig_details(conn, cig):
    """Ottiene tutti i dettagli di un CIG specifico."""
    details = {}
    
    # Informazioni base
    base_query = "SELECT * FROM cig WHERE cig = ?"
    details['base'] = pd.read_sql_query(base_query, conn, params=(cig,))
    
    # Bando
    bando_query = "SELECT * FROM bandi WHERE cig = ?"
    details['bando'] = pd.read_sql_query(bando_query, conn, params=(cig,))
    
    # Aggiudicazioni
    aggiudicazioni_query = "SELECT * FROM aggiudicazioni WHERE cig = ?"
    details['aggiudicazioni'] = pd.read_sql_query(aggiudicazioni_query, conn, params=(cig,))
    
    # Partecipanti
    partecipanti_query = "SELECT * FROM partecipanti WHERE cig = ?"
    details['partecipanti'] = pd.read_sql_query(partecipanti_query, conn, params=(cig,))
    
    # Varianti
    varianti_query = "SELECT * FROM varianti WHERE cig = ?"
    details['varianti'] = pd.read_sql_query(varianti_query, conn, params=(cig,))
    
    return details

def print_cig_details(details):
    """Stampa i dettagli di un CIG in formato tabellare."""
    if details['base'].empty:
        print("\n❌ CIG non trovato")
        return
    
    print("\n📋 Informazioni Base:")
    print(tabulate(details['base'], headers='keys', tablefmt='psql'))
    
    if not details['bando'].empty:
        print("\n📑 Informazioni Bando:")
        print(tabulate(details['bando'], headers='keys', tablefmt='psql'))
    
    if not details['aggiudicazioni'].empty:
        print("\n💰 Aggiudicazioni:")
        print(tabulate(details['aggiudicazioni'], headers='keys', tablefmt='psql'))
    
    if not details['partecipanti'].empty:
        print("\n👥 Partecipanti:")
        print(tabulate(details['partecipanti'], headers='keys', tablefmt='psql'))
    
    if not details['varianti'].empty:
        print("\n📝 Varianti:")
        print(tabulate(details['varianti'], headers='keys', tablefmt='psql'))

def export_results(df, format='csv'):
    """Esporta i risultati in un file."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if format == 'csv':
        filename = f'cig_export_{timestamp}.csv'
        df.to_csv(filename, index=False)
    elif format == 'excel':
        filename = f'cig_export_{timestamp}.xlsx'
        df.to_excel(filename, index=False)
    
    print(f"\n✅ Risultati esportati in: {filename}")

def main():
    # Verifica se il database esiste
    if not os.path.exists('database.db'):
        print("❌ Database non trovato. Esegui prima l'importazione dei dati.")
        sys.exit(1)
    
    # Connessione al database
    conn = sqlite3.connect('database.db')
    
    while True:
        clear_screen()
        print_header()
        
        # Mostra statistiche del database
        stats = get_db_stats(conn)
        print("📊 Statistiche Database:")
        print(f"  • CIG totali: {stats['cig']:,}")
        print(f"  • Bandi: {stats['bandi']:,}")
        print(f"  • Aggiudicazioni: {stats['aggiudicazioni']:,}")
        print(f"  • Partecipanti: {stats['partecipanti']:,}")
        print(f"  • Varianti: {stats['varianti']:,}")
        
        print("\n🔍 Menu Principale:")
        print("1. Cerca per codice CIG")
        print("2. Cerca per termine (CIG o oggetto)")
        print("3. Esporta risultati")
        print("4. Esci")
        
        choice = input("\nScelta: ")
        
        if choice == '1':
            cig = input("\nInserisci il codice CIG: ").strip()
            if cig:
                details = get_cig_details(conn, cig)
                print_cig_details(details)
                input("\nPremi Invio per continuare...")
        
        elif choice == '2':
            search_term = input("\nInserisci il termine di ricerca: ").strip()
            if search_term:
                results = search_cig(conn, search_term)
                if not results.empty:
                    print("\n🔍 Risultati della ricerca:")
                    print(tabulate(results, headers='keys', tablefmt='psql'))
                    
                    # Chiedi se vuoi vedere i dettagli
                    cig = input("\nInserisci il CIG per vedere i dettagli (o premi Invio per tornare): ").strip()
                    if cig:
                        details = get_cig_details(conn, cig)
                        print_cig_details(details)
                else:
                    print("\n❌ Nessun risultato trovato")
                input("\nPremi Invio per continuare...")
        
        elif choice == '3':
            print("\n📤 Esporta risultati:")
            print("1. Esporta in CSV")
            print("2. Esporta in Excel")
            print("3. Torna al menu principale")
            
            export_choice = input("\nScelta: ")
            
            if export_choice in ['1', '2']:
                search_term = input("\nInserisci il termine di ricerca: ").strip()
                if search_term:
                    results = search_cig(conn, search_term, limit=1000)  # Aumenta il limite per l'esportazione
                    if not results.empty:
                        format = 'csv' if export_choice == '1' else 'excel'
                        export_results(results, format)
                    else:
                        print("\n❌ Nessun risultato da esportare")
                input("\nPremi Invio per continuare...")
        
        elif choice == '4':
            print("\n👋 Arrivederci!")
            break
        
        else:
            print("\n❌ Scelta non valida")
            input("\nPremi Invio per continuare...")
    
    conn.close()

if __name__ == "__main__":
    main() 