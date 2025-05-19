#!/bin/bash

# Colori per output
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

# Avvia l'importer
echo -e "${YELLOW}🚀 Avvio importazione dati...${NC}"
python src/anac_importer.py

# Disattiva l'ambiente virtuale
deactivate

echo -e "${GREEN}✅ Script completato${NC}" 