#!/bin/bash

# Colori per i messaggi
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Ottieni il percorso assoluto dello script
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

# Verifica se lo script è eseguito con sudo
if [ "$EUID" -eq 0 ]; then
    echo -e "${YELLOW}⚠️ Script eseguito con sudo. Verifica dei permessi...${NC}"
    ORIGINAL_USER=$(logname)
    echo -e "${YELLOW}👤 Utente originale: $ORIGINAL_USER${NC}"
    echo -e "${YELLOW}🔄 Riavvio script come utente $ORIGINAL_USER...${NC}"
    exec su - "$ORIGINAL_USER" -c "bash '$SCRIPT_PATH'"
    exit
fi

# URL del repository
REPO_URL="https://github.com/pherdinauer/DBmake.git"

# Spostamento nella directory DBmake
echo -e "${YELLOW}📂 Spostamento nella directory DBmake...${NC}"
cd "$(dirname "$SCRIPT_PATH")"

# Verifica se siamo nella directory DBmake
if [ "$(basename $(pwd))" != "DBmake" ]; then
    echo -e "${YELLOW}📥 Directory DBmake non trovata. Clonazione in corso...${NC}"
    
    # Se la directory esiste ma non è un repository git valido
    if [ -d "DBmake" ]; then
        echo -e "${YELLOW}⚠️ Directory DBmake esiste ma non è un repository git valido${NC}"
        echo -e "${YELLOW}🗑️ Rimozione directory esistente...${NC}"
        rm -rf DBmake
    fi
    
    # Clona il repository
    git clone $REPO_URL DBmake
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Repository clonato con successo${NC}"
        cd DBmake
    else
        echo -e "${RED}❌ Errore durante la clonazione del repository${NC}"
        exit 1
    fi
fi

# Configurazione sicurezza Git
echo -e "${YELLOW}🔒 Configurazione sicurezza Git...${NC}"
git config --global --add safe.directory "$(pwd)"

# Verifica se siamo in un repository git
if [ ! -d ".git" ]; then
    echo -e "${RED}❌ Directory non è un repository git valido${NC}"
    exit 1
fi

# Assicurati di essere sul branch MULTITAB
echo -e "${YELLOW}🔄 Verifica e checkout branch MULTITAB...${NC}"
current_branch=$(git branch --show-current)
if [ "$current_branch" != "MULTITAB" ]; then
    echo -e "${YELLOW}📋 Branch attuale: $current_branch${NC}"
    echo -e "${YELLOW}🔄 Switching al branch MULTITAB...${NC}"
    if git checkout MULTITAB; then
        echo -e "${GREEN}✅ Checkout su branch MULTITAB completato${NC}"
    else
        echo -e "${RED}❌ Errore nel checkout su branch MULTITAB${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✅ Già sul branch MULTITAB${NC}"
fi

# Gestione delle modifiche locali
echo -e "${YELLOW}🔄 Gestione modifiche locali...${NC}"
if git diff --quiet 2>/dev/null; then
    echo -e "${GREEN}✅ Nessuna modifica locale da gestire${NC}"
else
    echo -e "${YELLOW}📦 Backup delle modifiche locali...${NC}"
    git stash save "Modifiche locali $(date '+%Y-%m-%d %H:%M:%S')"
fi

# Aggiornamento repository
echo -e "${YELLOW}🔄 Aggiornamento repository dal branch MULTITAB...${NC}"
if git pull origin MULTITAB; then
    echo -e "${GREEN}✅ Repository aggiornato con successo dal branch MULTITAB${NC}"
    
    # Verifica finale che siamo ancora sul branch MULTITAB
    final_branch=$(git branch --show-current)
    if [ "$final_branch" != "MULTITAB" ]; then
        echo -e "${YELLOW}⚠️ Branch cambiato durante il pull, ritorno a MULTITAB...${NC}"
        git checkout MULTITAB
    fi
    
    # Ripristino modifiche locali se presenti
    if git stash list | grep -q "Modifiche locali"; then
        echo -e "${YELLOW}🔄 Ripristino modifiche locali...${NC}"
        if git stash pop; then
            echo -e "${GREEN}✅ Modifiche locali ripristinate${NC}"
        else
            echo -e "${YELLOW}⚠️ Conflitti durante il ripristino delle modifiche locali${NC}"
            echo -e "${YELLOW}📋 Stato attuale:${NC}"
            git status
            echo -e "${RED}❌ Risolvi manualmente i conflitti e riprova${NC}"
            exit 1
        fi
    fi
else
    echo -e "${RED}❌ Errore durante l'aggiornamento del repository${NC}"
    exit 1
fi

# Gestione ambiente virtuale
echo -e "${YELLOW}🔧 Gestione ambiente virtuale...${NC}"

# Rimuovi l'ambiente virtuale esistente se presente
if [ -d "venv" ]; then
    echo -e "${YELLOW}🗑️ Rimozione ambiente virtuale esistente...${NC}"
    rm -rf venv
fi

# Crea nuovo ambiente virtuale
echo -e "${YELLOW}📦 Creazione nuovo ambiente virtuale...${NC}"
python3 -m venv venv

# Attiva l'ambiente virtuale
echo -e "${YELLOW}🔌 Attivazione ambiente virtuale...${NC}"
source venv/bin/activate

# Imposta PYTHONPATH per includere la directory corrente e src
export PYTHONPATH="$(pwd):$(pwd)/src:${PYTHONPATH}"

# Verifica che l'ambiente virtuale sia attivo
if [ -z "$VIRTUAL_ENV" ]; then
    echo -e "${RED}❌ Errore nell'attivazione dell'ambiente virtuale${NC}"
    exit 1
fi

# Aggiorna pip
echo -e "${YELLOW}📦 Aggiornamento pip...${NC}"
pip install --upgrade pip

# Installa le dipendenze
echo -e "${YELLOW}📦 Installazione dipendenze...${NC}"
pip install -r requirements.txt
# Installa anche mysql-connector-python se non presente
if ! python -c "import mysql.connector" 2>/dev/null; then
    echo -e "${YELLOW}📦 Installazione modulo mysql-connector-python...${NC}"
    pip install mysql-connector-python
    # Aggiungi a requirements.txt se non già presente
    if ! grep -q "mysql-connector-python" requirements.txt; then
        echo "mysql-connector-python" >> requirements.txt
    fi
fi

# Verifica l'installazione di pandas
echo -e "${YELLOW}🔍 Verifica installazione pandas...${NC}"
python3 -c "import pandas; print(f'✅ Pandas versione {pandas.__version__} installato correttamente')"

# Verifica che la directory /database sia montata
if ! mountpoint -q /database; then
    echo -e "${RED}❌ La directory /database non è montata${NC}"
    echo -e "${YELLOW}⚠️ Tentativo di montaggio...${NC}"
    sudo mount /dev/sdc3 /database
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Impossibile montare /database. Verifica i permessi e la configurazione${NC}"
        exit 1
    fi
fi

# Crea le directory necessarie se non esistono
mkdir -p logs database/backups

# Imposta i permessi
echo -e "${YELLOW}🔧 Impostazione permessi...${NC}"
sudo chown -R $USER:$USER .
chmod -R 755 .

# Funzione per mostrare il menu
show_menu() {
    clear
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════════════════╗"
    echo -e "║                    CIG Database Management Tool - AUTO-TURBO                   ║"
    echo -e "║                           Branch: MULTITAB (Ottimizzato)                       ║"
    echo -e "╚════════════════════════════════════════════════════════════════════════════╝${NC}"
    echo
    echo -e "${YELLOW}1)${NC} Importa dati in SQLite"
    echo -e "${YELLOW}2)${NC} Genera file SQL per MySQL"
    echo -e "${YELLOW}3)${NC} 🚀 Auto-Turbo MySQL Import (Consigliato)"
    echo -e "${YELLOW}4)${NC} Cerca CIG nel database"
    echo -e "${YELLOW}5)${NC} Esci"
    echo
    echo -n -e "${YELLOW}Scegli un'opzione (1-5): ${NC}"
}

# Funzione per importare in SQLite
import_to_sqlite() {
    echo -e "${YELLOW}Inizio importazione dati in SQLite...${NC}"
    python src/import_json_to_sqlite.py
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Importazione completata con successo!${NC}"
    else
        echo -e "${RED}Errore durante l'importazione.${NC}"
    fi
}

# Funzione per generare SQL MySQL
generate_mysql_sql() {
    echo -e "${YELLOW}Generazione file SQL per MySQL...${NC}"
    echo -e "${GREEN}Il chunk size ora è dinamico e adattato automaticamente alla RAM disponibile.${NC}"
    python src/export_to_mysql_sql.py
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}File SQL generato con successo!${NC}"
    else
        echo -e "${RED}Errore durante la generazione del file SQL.${NC}"
    fi
}

# Funzione per import diretto in MySQL
import_to_mysql() {
    echo -e "${YELLOW}🚀 Avvio Auto-Turbo MySQL Import...${NC}"
    echo -e "${GREEN}💪 Modalità HIGH-PERFORMANCE con rilevamento automatico risorse${NC}"
    echo -e "${GREEN}⚡ Ottimizzazioni: multi-thread, batch dinamici, schema intelligente${NC}"
    python src/import_json_mysql.py
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}🎉 Auto-Turbo Import completato con successo!${NC}"
    else
        echo -e "${RED}❌ Errore durante l'Auto-Turbo Import.${NC}"
    fi
}

# Funzione per cercare CIG
search_cig() {
    echo -e "${YELLOW}Avvio ricerca CIG...${NC}"
    read -p -e "${YELLOW}Inserisci il CIG da cercare: ${NC}" cig
    python src/search_cig.py "$cig"
}

# Loop principale
while true; do
    show_menu
    read choice

    case $choice in
        1)
            import_to_sqlite
            ;;
        2)
            generate_mysql_sql
            ;;
        3)
            import_to_mysql
            ;;
        4)
            search_cig
            ;;
        5)
            echo -e "${GREEN}Arrivederci!${NC}"
            deactivate
            exit 0
            ;;
        *)
            echo -e "${RED}Opzione non valida.${NC}"
            ;;
    esac

    echo
    echo -e "${YELLOW}Premi INVIO per tornare al menu principale...${NC}"
    read
done 