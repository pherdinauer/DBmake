import sqlite3
import pandas as pd
from tabulate import tabulate
from typing import Dict, List

def get_cig_info(conn: sqlite3.Connection, cig: str) -> Dict:
    """Ottiene tutte le informazioni relative a un CIG specifico."""
    # Informazioni base del CIG
    cig_query = """
    SELECT * FROM cig WHERE cig = ?
    """
    cig_info = pd.read_sql_query(cig_query, conn, params=(cig,))
    
    # Informazioni sul bando
    bandi_query = """
    SELECT * FROM bandi WHERE cig = ?
    """
    bandi_info = pd.read_sql_query(bandi_query, conn, params=(cig,))
    
    # Informazioni sulle aggiudicazioni
    aggiudicazioni_query = """
    SELECT * FROM aggiudicazioni WHERE cig = ?
    """
    aggiudicazioni_info = pd.read_sql_query(aggiudicazioni_query, conn, params=(cig,))
    
    # Informazioni sui partecipanti
    partecipanti_query = """
    SELECT * FROM partecipanti WHERE cig = ?
    """
    partecipanti_info = pd.read_sql_query(partecipanti_query, conn, params=(cig,))
    
    # Informazioni sulle varianti
    varianti_query = """
    SELECT * FROM varianti WHERE cig = ?
    """
    varianti_info = pd.read_sql_query(varianti_query, conn, params=(cig,))
    
    return {
        'cig': cig_info,
        'bandi': bandi_info,
        'aggiudicazioni': aggiudicazioni_info,
        'partecipanti': partecipanti_info,
        'varianti': varianti_info
    }

def search_cig(conn: sqlite3.Connection, search_term: str, limit: int = 10) -> pd.DataFrame:
    """Cerca CIG che corrispondono al termine di ricerca."""
    query = """
    SELECT c.*, 
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

def print_cig_info(info: Dict) -> None:
    """Stampa le informazioni di un CIG in formato tabellare."""
    print("\n📋 Informazioni CIG:")
    if not info['cig'].empty:
        print(tabulate(info['cig'], headers='keys', tablefmt='psql'))
    else:
        print("❌ CIG non trovato")
        return
    
    print("\n📑 Informazioni Bando:")
    if not info['bandi'].empty:
        print(tabulate(info['bandi'], headers='keys', tablefmt='psql'))
    
    print("\n💰 Aggiudicazioni:")
    if not info['aggiudicazioni'].empty:
        print(tabulate(info['aggiudicazioni'], headers='keys', tablefmt='psql'))
    
    print("\n👥 Partecipanti:")
    if not info['partecipanti'].empty:
        print(tabulate(info['partecipanti'], headers='keys', tablefmt='psql'))
    
    print("\n📝 Varianti:")
    if not info['varianti'].empty:
        print(tabulate(info['varianti'], headers='keys', tablefmt='psql'))

def main():
    # Connessione al database
    conn = sqlite3.connect('database.db')
    
    while True:
        print("\n🔍 Cerca un CIG:")
        print("1. Cerca per codice CIG esatto")
        print("2. Cerca per termine (CIG o oggetto)")
        print("3. Esci")
        
        choice = input("\nScelta: ")
        
        if choice == '1':
            cig = input("Inserisci il codice CIG: ")
            info = get_cig_info(conn, cig)
            print_cig_info(info)
            
        elif choice == '2':
            search_term = input("Inserisci il termine di ricerca: ")
            results = search_cig(conn, search_term)
            if not results.empty:
                print("\n🔍 Risultati della ricerca:")
                print(tabulate(results, headers='keys', tablefmt='psql'))
                
                # Chiedi se vuoi vedere i dettagli di un CIG specifico
                cig = input("\nInserisci il CIG per vedere i dettagli (o premi Invio per tornare): ")
                if cig:
                    info = get_cig_info(conn, cig)
                    print_cig_info(info)
            else:
                print("❌ Nessun risultato trovato")
                
        elif choice == '3':
            break
        
        else:
            print("❌ Scelta non valida")
    
    conn.close()

if __name__ == "__main__":
    main() 