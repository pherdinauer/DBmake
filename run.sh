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

# Assicurati di essere sul branch cursor/crea-un-nuovo-branch-agent-8d8c
echo -e "${YELLOW}🔄 Verifica e checkout branch cursor/crea-un-nuovo-branch-agent-8d8c...${NC}"
current_branch=$(git branch --show-current)
if [ "$current_branch" != "cursor/crea-un-nuovo-branch-agent-8d8c" ]; then
    echo -e "${YELLOW}📋 Branch attuale: $current_branch${NC}"
    echo -e "${YELLOW}🔄 Switching al branch cursor/crea-un-nuovo-branch-agent-8d8c...${NC}"
    if git checkout "cursor/crea-un-nuovo-branch-agent-8d8c"; then
        echo -e "${GREEN}✅ Checkout su branch cursor/crea-un-nuovo-branch-agent-8d8c completato${NC}"
    else
        echo -e "${RED}❌ Errore nel checkout su branch cursor/crea-un-nuovo-branch-agent-8d8c${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✅ Già sul branch cursor/crea-un-nuovo-branch-agent-8d8c${NC}"
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
echo -e "${YELLOW}🔄 Aggiornamento repository dal branch cursor/crea-un-nuovo-branch-agent-8d8c...${NC}"
if git pull origin "cursor/crea-un-nuovo-branch-agent-8d8c"; then
    echo -e "${GREEN}✅ Repository aggiornato con successo dal branch cursor/crea-un-nuovo-branch-agent-8d8c${NC}"
    
    # Verifica finale che siamo ancora sul branch cursor/crea-un-nuovo-branch-agent-8d8c
    final_branch=$(git branch --show-current)
    if [ "$final_branch" != "cursor/crea-un-nuovo-branch-agent-8d8c" ]; then
        echo -e "${YELLOW}⚠️ Branch cambiato durante il pull, ritorno a cursor/crea-un-nuovo-branch-agent-8d8c...${NC}"
        git checkout "cursor/crea-un-nuovo-branch-agent-8d8c"
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
    echo -e "║              ANAC Import JSON - 3 SOLUZIONI DINAMICHE AVANZATE                ║"
    echo -e "║           Branch: cursor/crea-un-nuovo-branch-agent-8d8c (Sviluppo)           ║"
    echo -e "╚════════════════════════════════════════════════════════════════════════════╝${NC}"
    echo
    echo -e "${RED}🎯 ONE-CLICK (NON TI DEVI PREOCCUPARE DI NIENTE!):${NC}"
    echo -e "${GREEN}0)${NC} 🎯 ONE-CLICK COMPLETO - FA TUTTO AUTOMATICAMENTE!"
    echo
    echo -e "${GREEN}🎯 COMANDI PRINCIPALI (MANUALI):${NC}"
    echo -e "${YELLOW}1)${NC} 🧪 Test Categorizzazione (FAI SEMPRE PRIMA!)"
    echo -e "${YELLOW}2)${NC} 🚀 SMART Import - Migliore per la tua struttura"
    echo -e "${YELLOW}3)${NC} 🔧 Fix Tabelle Vuote (risolve il problema)"
    echo
    echo -e "${GREEN}🔧 MODALITÀ ALTERNATIVE:${NC}"
    echo -e "${YELLOW}4)${NC} 🌊 Streaming Import (dataset enormi >10GB)"
    echo -e "${YELLOW}5)${NC} 🤖 Auto-Mode (rileva automaticamente)"
    echo -e "${YELLOW}6)${NC} 📋 Modalità Standard (pattern classici)"
    echo
    echo -e "${GREEN}📊 UTILITÀ:${NC}"
    echo -e "${YELLOW}7)${NC} 📊 Status Sistema e Configurazione"
    echo -e "${YELLOW}8)${NC} 📄 Mostra Ultimi Log"
    echo -e "${YELLOW}9)${NC} 🧪 Test Tutte le Soluzioni"
    echo
    echo -e "${GREEN}🗂️ LEGACY:${NC}"
    echo -e "${YELLOW}10)${NC} Importa dati in SQLite"
    echo -e "${YELLOW}11)${NC} Genera file SQL per MySQL"
    echo -e "${YELLOW}12)${NC} 🛡️ Verifica Integrità Database"
    echo -e "${YELLOW}13)${NC} Cerca CIG nel database"
    echo -e "${YELLOW}99)${NC} Esci"
    echo
    echo -e "${RED}💡 RACCOMANDAZIONE SUPER: USA OPZIONE 0 (ONE-CLICK) E NON TI PREOCCUPARE DI NIENTE!${NC}"
    echo -e "${GREEN}💡 Alternative manuali: Usa opzioni 1 → 2 per controllo manuale${NC}"
    echo
    echo -n -e "${YELLOW}Scegli un'opzione (0-13, 99): ${NC}"
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

# 🧪 FUNZIONI NUOVE CON CLI UNIFICATO

# 🎯 ONE-CLICK COMPLETO - FA TUTTO AUTOMATICAMENTE!
one_click_complete() {
    echo -e "${RED}🎯 ONE-CLICK COMPLETO - MODALITÀ AUTOMATICA TOTALE${NC}"
    echo -e "${GREEN}✨ Non ti devi preoccupare di niente! Farò tutto io:${NC}"
    echo -e "${GREEN}   ✅ Test automatico categorizzazione${NC}"
    echo -e "${GREEN}   ✅ Import automatico con SMART mode${NC}"
    echo -e "${GREEN}   ✅ Fix automatico se ci sono problemi${NC}"
    echo -e "${GREEN}   ✅ Verifica finale del risultato${NC}"
    echo
    
    # Chiedi direttamente il nome del database
    echo -e "${YELLOW}🗄️ Nome Database MySQL:${NC}"
    read -p "Inserisci nome database (INVIO per default 'anac_import3'): " custom_db
    
    # Se vuoto, usa default
    if [ -z "$custom_db" ]; then
        custom_db="anac_import3"
        echo -e "${GREEN}🔧 Uso database di default: $custom_db${NC}"
    else
        echo -e "${GREEN}🔧 Uso database personalizzato: $custom_db${NC}"
    fi
    
    echo
    echo -e "${YELLOW}🚀 Avvio processo completo automatico...${NC}"
    echo -e "${CYAN}📋 Target: $custom_db @ localhost${NC}"
    echo
    
    # Avvia ONE-CLICK con database specificato
    python run_import.py --one-click --database "$custom_db" --force
    
    if [ $? -eq 0 ]; then
        echo
        echo -e "${GREEN}🎉 ONE-CLICK SUCCESS: Tutto completato automaticamente!${NC}"
        echo -e "${GREEN}✅ Database '$custom_db' ora contiene tutti i tuoi dati!${NC}"
        echo -e "${GREEN}🎯 Problema tabelle vuote RISOLTO!${NC}"
    else
        echo
        echo -e "${RED}💔 ONE-CLICK FAILED: Si è verificato un problema${NC}"
        echo -e "${YELLOW}🆘 Ma non preoccuparti! Prova una di queste opzioni:${NC}"
        echo -e "${YELLOW}   - Opzione 4 (Streaming Import) per dataset grandi${NC}"
        echo -e "${YELLOW}   - Opzione 9 (Test tutte le soluzioni) per diagnostica${NC}"
        echo -e "${YELLOW}   - Opzione 8 (Mostra log) per vedere errori dettagliati${NC}"
    fi
}

# Test categorizzazione
test_categorization() {
    echo -e "${YELLOW}🧪 Avvio Test Categorizzazione...${NC}"
    echo -e "${GREEN}Verifica che i file JSON vengano trovati e categorizzati correttamente${NC}"
    echo -e "${GREEN}(Questo test NON importa nulla, solo verifica)${NC}"
    echo
    
    python run_import.py --test
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Test categorizzazione completato con successo!${NC}"
        echo -e "${GREEN}💡 Ora puoi procedere con l'importazione usando opzione 2${NC}"
    else
        echo -e "${RED}❌ Test categorizzazione fallito.${NC}"
        echo -e "${YELLOW}💡 Controlla i path JSON e i file prima di procedere${NC}"
    fi
}

# SMART Import - Migliore per la struttura dell'utente
smart_import() {
    echo -e "${YELLOW}🧠 Avvio SMART Import - Migliore per la tua struttura...${NC}"
    echo -e "${GREEN}✨ Caratteristiche SMART mode:${NC}"
    echo -e "${GREEN}   - Analizza contenuto file oltre ai nomi${NC}"
    echo -e "${GREEN}   - Gestisce timestamp complessi (YYYYMMDD-)${NC}"
    echo -e "${GREEN}   - Recovery automatico file non categorizzati${NC}"
    echo -e "${GREEN}   - Velocità ottimale per dataset medi${NC}"
    echo
    
    # Chiedi modalità di configurazione database
    echo -e "${YELLOW}🔧 Configurazione Database:${NC}"
    echo -e "${YELLOW}1)${NC} Usa database di default (anac_import3)"
    echo -e "${YELLOW}2)${NC} Specifica database personalizzato"
    echo -e "${YELLOW}3)${NC} Modalità automatica (salta conferme)"
    echo
    read -p "Scegli opzione (1-3): " db_choice
    
    case $db_choice in
        1)
            echo -e "${GREEN}🔧 Utilizzo database di default${NC}"
            python run_import.py --run
            ;;
        2)
            read -p "Inserisci nome database personalizzato: " custom_db
            if [ -n "$custom_db" ]; then
                echo -e "${GREEN}🔧 Utilizzo database personalizzato: $custom_db${NC}"
                python run_import.py --run --database "$custom_db"
            else
                echo -e "${RED}❌ Nome database non valido, uso database di default${NC}"
                python run_import.py --run
            fi
            ;;
        3)
            echo -e "${GREEN}⚡ Modalità automatica attivata${NC}"
            python run_import.py --run --force
            ;;
        *)
            echo -e "${GREEN}🔧 Utilizzo database di default (opzione non valida)${NC}"
            python run_import.py --run
            ;;
    esac
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}🎉 SMART Import completato con successo!${NC}"
        echo -e "${GREEN}✅ Le tabelle dovrebbero ora contenere i tuoi dati${NC}"
    else
        echo -e "${RED}❌ Errore durante SMART Import.${NC}"
        echo -e "${YELLOW}💡 Prova l'opzione 3 (Fix Tabelle Vuote) per risolvere${NC}"
    fi
}

# Fix tabelle vuote
fix_empty_tables() {
    echo -e "${YELLOW}🔧 Avvio Fix Tabelle Vuote...${NC}"
    echo -e "${GREEN}🎯 Questo risolve specificamente il problema delle tabelle vuote${NC}"
    echo -e "${GREEN}   - Prima testa la categorizzazione${NC}"
    echo -e "${GREEN}   - Poi usa SMART mode per l'importazione${NC}"
    echo -e "${GREEN}   - Gestione automatica degli errori${NC}"
    echo
    
    python run_import.py --fix-empty
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}🎉 Fix tabelle vuote completato con successo!${NC}"
        echo -e "${GREEN}✅ Il problema dovrebbe essere risolto${NC}"
    else
        echo -e "${RED}❌ Errore durante il fix delle tabelle vuote.${NC}"
        echo -e "${YELLOW}💡 Verifica i log con l'opzione 8${NC}"
    fi
}

# Streaming import per dataset enormi
streaming_import() {
    echo -e "${YELLOW}🌊 Avvio Streaming Import...${NC}"
    echo -e "${GREEN}💪 Modalità STREAMING per dataset enormi:${NC}"
    echo -e "${GREEN}   - Processing incrementale file-by-file${NC}"
    echo -e "${GREEN}   - Auto-retry automatico su errori${NC}"
    echo -e "${GREEN}   - Recovery intelligente da interruzioni${NC}"
    echo -e "${GREEN}   - Gestione memoria ottimizzata${NC}"
    echo
    
    python run_import.py --streaming
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}🎉 Streaming Import completato con successo!${NC}"
    else
        echo -e "${RED}❌ Errore durante Streaming Import.${NC}"
    fi
}

# Auto-mode (rileva automaticamente)
auto_import() {
    echo -e "${YELLOW}🤖 Avvio Auto-Mode...${NC}"
    echo -e "${GREEN}🧠 Rilevamento automatico modalità ottimale:${NC}"
    echo -e "${GREEN}   - Dataset piccolo (<100 file): STANDARD${NC}"
    echo -e "${GREEN}   - Dataset medio (100-1000 file): SMART${NC}"
    echo -e "${GREEN}   - Dataset grande (>1000 file): STREAMING${NC}"
    echo
    
    python run_import.py --auto
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}🎉 Auto-Mode completato con successo!${NC}"
    else
        echo -e "${RED}❌ Errore durante Auto-Mode.${NC}"
    fi
}

# Modalità standard
standard_import() {
    echo -e "${YELLOW}📋 Avvio Modalità Standard...${NC}"
    echo -e "${GREEN}⚙️ Modalità Standard con pattern classici${NC}"
    echo
    
    python run_import.py --standard
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}🎉 Modalità Standard completata con successo!${NC}"
    else
        echo -e "${RED}❌ Errore durante Modalità Standard.${NC}"
    fi
}

# Status sistema
show_status() {
    echo -e "${YELLOW}📊 Status Sistema e Configurazione...${NC}"
    echo
    
    python run_import.py --status
}

# Mostra ultimi log
show_logs() {
    echo -e "${YELLOW}📄 Ultimi Log di Importazione...${NC}"
    echo
    
    python run_import.py --logs
}

# Test tutte le soluzioni
test_all_solutions() {
    echo -e "${YELLOW}🧪 Test di tutte le 3 Soluzioni Dinamiche...${NC}"
    echo -e "${GREEN}Questo testa tutte le soluzioni implementate${NC}"
    echo
    
    python run_import.py --test-all
}

# 🗂️ FUNZIONI LEGACY (mantenute per compatibilità)

# Funzione per import diretto in MySQL (LEGACY)
legacy_import_to_mysql() {
    echo -e "${YELLOW}🚀 Avvio Auto-Turbo MySQL Import (LEGACY)...${NC}"
    echo -e "${RED}⚠️ NOTA: Questa è la versione LEGACY. Usa le opzioni 1-6 per le nuove funzioni${NC}"
    echo -e "${GREEN}💪 Modalità HIGH-PERFORMANCE con rilevamento automatico risorse${NC}"
    echo
    
    # Chiedi modalità di configurazione database
    echo -e "${YELLOW}🔧 Configurazione Database:${NC}"
    echo -e "${YELLOW}1)${NC} Usa database di default (anac_import3)"
    echo -e "${YELLOW}2)${NC} 🆕 Modalità interattiva (crea nuovo o scegli esistente)"
    echo -e "${YELLOW}3)${NC} Specifica database personalizzato direttamente"
    echo
    read -p "Scegli opzione (1-3): " db_choice
    
    case $db_choice in
        1)
            echo -e "${GREEN}🔧 Utilizzo database di default${NC}"
            python src/mysql_import_wrapper.py
            ;;
        2)
            echo -e "${CYAN}🆕 Modalità interattiva attivata...${NC}"
            echo -e "${CYAN}Ti verrà chiesto di scegliere o creare un database${NC}"
            python src/mysql_import_wrapper.py --interactive
            ;;
        3)
            read -p "Inserisci nome database personalizzato: " custom_db
            if [ -n "$custom_db" ]; then
                echo -e "${GREEN}🔧 Utilizzo database personalizzato: $custom_db${NC}"
                python src/mysql_import_wrapper.py --database "$custom_db"
            else
                echo -e "${RED}❌ Nome database non valido, uso database di default${NC}"
                python src/mysql_import_wrapper.py
            fi
            ;;
        *)
            echo -e "${GREEN}🔧 Utilizzo database di default (opzione non valida)${NC}"
            python src/mysql_import_wrapper.py
            ;;
    esac
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}🎉 Auto-Turbo Import (LEGACY) completato con successo!${NC}"
    else
        echo -e "${RED}❌ Errore durante l'Auto-Turbo Import (LEGACY).${NC}"
    fi
}

# Funzione per verifica integrità database
check_database_integrity() {
    echo -e "${YELLOW}🛡️ Avvio Verifica Integrità Database...${NC}"
    echo -e "${GREEN}Controllo consistenza e qualità dei dati nelle tabelle MySQL${NC}"
    
    # Chiedi modalità di configurazione database
    echo
    echo -e "${YELLOW}🔧 Configurazione Database da verificare:${NC}"
    echo -e "${YELLOW}1)${NC} Verifica database di default (anac_import3)"
    echo -e "${YELLOW}2)${NC} Specifica database personalizzato"
    echo -e "${YELLOW}3)${NC} Modalità verbosa (output dettagliato)"
    echo
    read -p "Scegli opzione (1-3): " integrity_choice
    
    case $integrity_choice in
        1)
            echo -e "${GREEN}🔧 Verifica database di default${NC}"
            python check_database_integrity.py
            ;;
        2)
            read -p "Inserisci nome database da verificare: " custom_db
            if [ -n "$custom_db" ]; then
                echo -e "${GREEN}🔧 Verifica database personalizzato: $custom_db${NC}"
                python check_database_integrity.py --database "$custom_db"
            else
                echo -e "${RED}❌ Nome database non valido, uso database di default${NC}"
                python check_database_integrity.py
            fi
            ;;
        3)
            echo -e "${CYAN}🔍 Modalità verbosa attivata...${NC}"
            read -p "Database da verificare (INVIO per default): " verbose_db
            if [ -n "$verbose_db" ]; then
                python check_database_integrity.py --database "$verbose_db" --verbose
            else
                python check_database_integrity.py --verbose
            fi
            ;;
        *)
            echo -e "${GREEN}🔧 Verifica database di default (opzione non valida)${NC}"
            python check_database_integrity.py
            ;;
    esac
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Verifica integrità completata: NESSUN PROBLEMA RILEVATO${NC}"
    elif [ $? -eq 1 ]; then
        echo -e "${YELLOW}⚠️ Verifica completata: alcuni problemi rilevati (vedi dettagli sopra)${NC}"
    else
        echo -e "${RED}❌ Errore durante la verifica dell'integrità.${NC}"
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
        0)
            one_click_complete
            ;;
        1)
            test_categorization
            ;;
        2)
            smart_import
            ;;
        3)
            fix_empty_tables
            ;;
        4)
            streaming_import
            ;;
        5)
            auto_import
            ;;
        6)
            standard_import
            ;;
        7)
            show_status
            ;;
        8)
            show_logs
            ;;
        9)
            test_all_solutions
            ;;
        10)
            import_to_sqlite
            ;;
        11)
            generate_mysql_sql
            ;;
        12)
            check_database_integrity
            ;;
        13)
            search_cig
            ;;
        99)
            echo -e "${GREEN}Arrivederci!${NC}"
            deactivate
            exit 0
            ;;
        # Compatibilità con vecchio sistema (per chi digita 3 aspettandosi MySQL import)
        old3|legacy)
            legacy_import_to_mysql
            ;;
        *)
            echo -e "${RED}Opzione non valida. Usa numeri da 0 a 13 o 99.${NC}"
            echo -e "${RED}💡 TIP SUPER: Usa opzione 0 (ONE-CLICK) per non preoccuparti di niente!${NC}"
            echo -e "${YELLOW}💡 TIP alternative: Usa opzioni 1 → 2 per controllo manuale${NC}"
            ;;
    esac

    echo
    echo -e "${YELLOW}Premi INVIO per tornare al menu principale...${NC}"
    read
done 