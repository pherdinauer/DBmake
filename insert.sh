#!/bin/bash

# Colori
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

BRANCH="INSERT"

# Verifica presenza modifiche locali
if git diff --quiet 2>/dev/null; then
    echo -e "${GREEN}✅ Nessuna modifica locale da gestire${NC}"
else
    echo -e "${YELLOW} Backup delle modifiche locali...${NC}"
    git stash save "Modifiche locali $(date '+%Y-%m-%d %H:%M:%S')"
fi

# Assicurati di essere sul branch corretto
echo -e "${YELLOW} Passaggio al branch '$BRANCH'...${NC}"
if git show-ref --verify --quiet refs/heads/$BRANCH; then
    git checkout $BRANCH
else
    echo -e "${YELLOW}📦 Branch '$BRANCH' non trovato in locale, provo a recuperarlo da remoto...${NC}"
    if git ls-remote --exit-code --heads origin $BRANCH >/dev/null 2>&1; then
        git checkout -b $BRANCH origin/$BRANCH
    else
        echo -e "${RED}❌ Il branch '$BRANCH' non esiste né in locale né in remoto.${NC}"
        exit 1
    fi
fi

# Aggiornamento repository
echo -e "${YELLOW} Aggiornamento repository...${NC}"
if git pull origin $BRANCH; then
    echo -e "${GREEN}✅ Repository aggiornato con successo${NC}"

    # Ripristino modifiche locali se presenti
    if git stash list | grep -q "Modifiche locali"; then
        echo -e "${YELLOW} Ripristino modifiche locali...${NC}"
        if git stash pop; then
            echo -e "${GREEN}✅ Modifiche locali ripristinate${NC}"
        else
            echo -e "${YELLOW}⚠️ Conflitti durante il ripristino delle modifiche locali${NC}"
            echo -e "${YELLOW} Stato attuale:${NC}"
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
echo -e "${YELLOW} Gestione ambiente virtuale...${NC}"

# Rimuovi l'ambiente virtuale esistente se presente
if [ -d "venv" ]; then
    echo -e "${YELLOW}️  Rimozione ambiente virtuale esistente...${NC}"
    rm -rf venv
fi

# Crea nuovo ambiente virtuale
echo -e "${YELLOW} Creazione nuovo ambiente virtuale...${NC}"
python3 -m venv venv

# Attiva l'ambiente virtuale
echo -e "${YELLOW} Attivazione ambiente virtuale...${NC}"
source venv/bin/activate

# Imposta PYTHONPATH per includere la directory corrente e src
export PYTHONPATH="$(pwd):$(pwd)/src:${PYTHONPATH}"

# Verifica che l'ambiente virtuale sia attivo
if [ -z "$VIRTUAL_ENV" ]; then
    echo -e "${RED}❌ Errore nell'attivazione dell'ambiente virtuale${NC}"
    exit 1
fi
