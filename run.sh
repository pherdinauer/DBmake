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

# Gestione delle modifiche locali
echo -e "${YELLOW}🔄 Gestione modifiche locali...${NC}"
if git diff --quiet 2>/dev/null; then
    echo -e "${GREEN}✅ Nessuna modifica locale da gestire${NC}"
else
    echo -e "${YELLOW}📦 Backup delle modifiche locali...${NC}"
    git stash save "Modifiche locali $(date '+%Y-%m-%d %H:%M:%S')"
fi

# Aggiornamento repository
echo -e "${YELLOW}🔄 Aggiornamento repository...${NC}"
if git pull; then
    echo -e "${GREEN}✅ Repository aggiornato con successo${NC}"
    
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
    echo -e "║                    CIG Database Management Tool - Auto-Turbo                   ║"
    echo -e "╚════════════════════════════════════════════════════════════════════════════╝${NC}"
    echo
    echo -e "${YELLOW}1)${NC} Importa dati in SQLite"
    echo -e "${YELLOW}2)${NC} Genera file SQL per MySQL"
    echo -e "${YELLOW}3)${NC} 🚀 Auto-Turbo MySQL Import (rilevamento automatico performance)"
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
    echo -e "${YELLOW}🤖 Auto-Turbo MySQL Import (rilevamento automatico performance)...${NC}"
    
    # Auto-detect risorse sistema
    echo -e "${YELLOW}🔍 Rilevamento automatico risorse sistema...${NC}"
    
    # Rileva CPU cores
    if command -v nproc &> /dev/null; then
        CPU_CORES=$(nproc)
        echo -e "${GREEN}   🔥 CPU cores rilevati: $CPU_CORES${NC}"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        CPU_CORES=$(sysctl -n hw.ncpu)
        echo -e "${GREEN}   🔥 CPU cores rilevati: $CPU_CORES (macOS)${NC}"
    else
        CPU_CORES=4
        echo -e "${YELLOW}   ⚠️  CPU non rilevabile, assumo: $CPU_CORES${NC}"
    fi
    
    # Rileva RAM
    if command -v free &> /dev/null; then
        RAM_GB=$(free -g | awk '/^Mem:/{print $2}')
        echo -e "${GREEN}   💾 RAM totale rilevata: ${RAM_GB}GB${NC}"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        RAM_BYTES=$(sysctl -n hw.memsize)
        RAM_GB=$((RAM_BYTES / 1024 / 1024 / 1024))
        echo -e "${GREEN}   💾 RAM totale rilevata: ${RAM_GB}GB (macOS)${NC}"
    else
        RAM_GB=8
        echo -e "${YELLOW}   ⚠️  RAM non rilevabile, assumo: ${RAM_GB}GB${NC}"
    fi
    
    # Load average (se disponibile)
    if command -v uptime &> /dev/null; then
        LOAD=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | sed 's/,//')
        echo -e "${GREEN}   📊 Load average: $LOAD${NC}"
    fi
    
    echo
    
    # Auto-detect modalità performance
    if [[ $CPU_CORES -ge 8 && $RAM_GB -ge 12 ]]; then
        MODE="HIGH-PERFORMANCE"
        ICON="💪"
        export SCHEMA_ANALYSIS_MODE="ultra-fast"
        echo -e "${GREEN}$ICON MODALITÀ AUTO-RILEVATA: $MODE${NC}"
        echo -e "${GREEN}   ⚡ Sistema potente → Performance massime!${NC}"
        echo -e "${GREEN}   🚀 CPU: Hyperthreading attivo ($CPU_CORES × 2 thread)${NC}"
        echo -e "${GREEN}   📦 Chunk: 200K-500K record${NC}"
        echo -e "${GREEN}   💾 INSERT: Batch triplicati (fino a 3M)${NC}"
        echo -e "${GREEN}   🔗 MySQL: Multi-processo + pool esteso${NC}"
    else
        MODE="STANDARD OTTIMIZZATO"
        ICON="🏃"
        export SCHEMA_ANALYSIS_MODE="fast"
        echo -e "${YELLOW}$ICON MODALITÀ AUTO-RILEVATA: $MODE${NC}"
        echo -e "${YELLOW}   ✨ Sistema standard → Configurazione ottimizzata!${NC}"
        echo -e "${YELLOW}   🔥 CPU: Thread aggressivi ($CPU_CORES thread)${NC}"
        echo -e "${YELLOW}   📦 Chunk: Dinamici basati su RAM${NC}"
        echo -e "${YELLOW}   💾 INSERT: Batch ottimizzati${NC}"
    fi
    
    echo
    echo -e "${YELLOW}🚀 Avvio import automatico...${NC}"
    echo -e "${YELLOW}📊 Modalità: $ICON $MODE${NC}"
    echo -e "${YELLOW}⏹️  Interruzione: Ctrl+C${NC}"
    echo
    
    # Timer
    START_TIME=$(date +%s)
    
    # Esecuzione con auto-turbo
    python src/import_json_mysql.py
    
    # Calcolo tempo e risultato
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    echo
    echo -e "${YELLOW}⏱️  TEMPO TOTALE: $((DURATION / 60))m $((DURATION % 60))s${NC}"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}🎉 IMPORT AUTO-TURBO COMPLETATO!${NC}"
        echo -e "${GREEN}✅ $MODE ha funzionato perfettamente!${NC}"
        echo -e "${GREEN}📈 Performance: $((DURATION / 60))min per l'elaborazione${NC}"
    else
        echo -e "${RED}❌ Errore durante l'importazione in MySQL.${NC}"
        echo -e "${YELLOW}💡 Verifica i log per maggiori dettagli${NC}"
        
        # Offri auto-fix se c'è errore
        echo
        echo -e "${YELLOW}🔧 Vuoi provare l'auto-fix per tabelle duplicate? (s/N): ${NC}"
        read -n 1 -r
        echo
        if [[ $REPLY =~ ^[Ss]$ ]]; then
            echo -e "${YELLOW}🔧 Esecuzione auto-fix...${NC}"
            python -m src.fix_duplicate_keys --force
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}✅ Auto-fix completato! Riprova l'import.${NC}"
            else
                echo -e "${RED}❌ Auto-fix fallito.${NC}"
            fi
        fi
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