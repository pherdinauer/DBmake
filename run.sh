#!/bin/bash

# Colori per output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Ottieni il percorso assoluto dello script
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"

# Verifica se lo script è stato eseguito con sudo
if [ "$EUID" -eq 0 ]; then
    echo "⚠️  Script eseguito con sudo, riavvio come utente originale..."
    ORIGINAL_USER=$(logname)
    echo "👤 Utente originale: $ORIGINAL_USER"
    
    # Riavvia lo script come utente originale
    exec su - "$ORIGINAL_USER" -c "bash '$SCRIPT_PATH' $@"
    exit
fi

# Funzione per mostrare l'help
show_help() {
    echo "Uso: $0 [opzioni]"
    echo ""
    echo "Opzioni:"
    echo "  --extract     Estrae i file ZIP dalle cartelle"
    echo "  --import      Importa i file JSON nel database"
    echo "  --all         Esegue sia l'estrazione che l'importazione"
    echo "  --help        Mostra questo messaggio di aiuto"
    echo ""
    echo "Esempi:"
    echo "  $0 --extract     # Estrae solo i file ZIP"
    echo "  $0 --import      # Importa solo i file JSON"
    echo "  $0 --all         # Esegue entrambe le operazioni"
}

# Gestione degli argomenti
if [ $# -eq 0 ]; then
    show_help
    exit 1
fi

# Verifica se è stata richiesta l'help
if [ "$1" == "--help" ]; then
    show_help
    exit 0
fi

# Verifica se gli argomenti sono validi
VALID_ARGS=("--extract" "--import" "--all")
VALID_ARG=false
for arg in "${VALID_ARGS[@]}"; do
    if [ "$1" == "$arg" ]; then
        VALID_ARG=true
        break
    fi
done

if [ "$VALID_ARG" = false ]; then
    echo "❌ Argomento non valido: $1"
    show_help
    exit 1
fi

# Sposta nella directory dello script
cd "$SCRIPT_DIR"

# Verifica se la directory è un repository git
if [ ! -d ".git" ]; then
    echo "⚠️  Directory non inizializzata come repository git"
    
    # Verifica se la directory esiste e non è vuota
    if [ -d "DBmake" ] && [ "$(ls -A DBmake)" ]; then
        echo "🗑️  Rimozione directory DBmake esistente..."
        rm -rf DBmake
    fi
    
    echo "📥 Clonazione repository..."
    git clone https://github.com/desiderato/DBmake.git
    cd DBmake
else
    # Configura git per la directory corrente
    git config --global --add safe.directory "$(pwd)"
    
    # Verifica se ci sono modifiche locali
    if ! git diff --quiet 2>/dev/null; then
        echo "💾 Backup modifiche locali..."
        git stash save "Modifiche locali prima del pull"
    fi
    
    # Aggiorna il repository
    echo "📥 Aggiornamento repository..."
    git pull
    
    # Ripristina le modifiche locali se presenti
    if git stash list | grep -q "Modifiche locali prima del pull"; then
        echo "📤 Ripristino modifiche locali..."
        if ! git stash pop; then
            echo "⚠️  Conflitti durante il ripristino delle modifiche locali"
            echo "📝 Risolvi manualmente i conflitti e poi esegui:"
            echo "   git stash drop"
            exit 1
        fi
    fi
fi

# Crea e attiva l'ambiente virtuale se non esiste
if [ ! -d "venv" ]; then
    echo "🔧 Creazione ambiente virtuale..."
    python -m venv venv
fi

# Attiva l'ambiente virtuale
echo "🔌 Attivazione ambiente virtuale..."
source venv/bin/activate

# Installa/aggiorna le dipendenze
echo "📦 Installazione dipendenze..."
pip install -r requirements.txt

# Esegui lo script Python con gli argomenti passati
echo "🚀 Avvio importazione dati..."
python src/anac_importer.py "$@"

# Disattiva l'ambiente virtuale
deactivate

echo -e "${GREEN}✅ Script completato${NC}" 