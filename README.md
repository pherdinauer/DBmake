# ANAC Data Importer

Questo progetto gestisce l'importazione di dati JSON dal portale ANAC (Autorità Nazionale Anticorruzione) in un database SQLite.

## Caratteristiche

- Importazione automatica di file JSON da cartelle predefinite
- Normalizzazione dei dati in tabelle relazionali
- Sistema di backup automatico
- Logging dettagliato delle operazioni
- Validazione dei dati
- Gestione degli errori robusta
- Interfaccia a menu per la gestione delle operazioni

## Struttura del Progetto

```
.
├── config/
│   └── config.py           # Configurazioni del progetto
├── src/
│   ├── import_json.py      # Importazione dati JSON
│   └── cig_cli.py         # Interfaccia di ricerca CIG
├── logs/                   # Directory per i log
├── database/              # Directory per il database
│   └── backups/          # Directory per i backup
├── requirements.txt       # Dipendenze Python
├── run.sh                # Script di setup e menu principale
└── README.md             # Questo file
```

## Installazione

1. Clona il repository
2. Esegui lo script di setup:
   ```bash
   ./run.sh
   ```
   Lo script si occuperà di:
   - Configurare l'ambiente virtuale
   - Installare le dipendenze
   - Verificare i permessi
   - Mostrare il menu di gestione

## Configurazione

Le configurazioni sono gestite tramite variabili d'ambiente o direttamente nel file `config/config.py`:

- `ANAC_BASE_PATH`: Percorso base dei file JSON
- `ANAC_DB_PATH`: Percorso del database SQLite
- `ANAC_BACKUP_PATH`: Percorso per i backup

## Utilizzo

Esegui lo script principale:

```bash
./run.sh
```

Il menu ti permetterà di:
1. Importare dati JSON nel database
2. Cercare CIG nel database
3. Uscire dal programma

## Logging

I log vengono salvati nella directory `logs/` con il formato:
```
YYYY-MM-DD HH:MM:SS - MODULE - LEVEL - MESSAGE
```

## Backup

Il sistema crea automaticamente backup del database prima di ogni importazione. I backup vengono mantenuti per 7 giorni (configurabile in `config.py`).

## Validazione Dati

Il sistema verifica la presenza di colonne obbligatorie per ogni tabella. Le colonne richieste sono configurate in `config.py`.

## Contribuire

1. Fork il repository
2. Crea un branch per la tua feature
3. Commit le tue modifiche
4. Push al branch
5. Crea una Pull Request

## Licenza

MIT 