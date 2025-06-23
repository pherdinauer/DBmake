# Utilizzo della funzionalità Database Personalizzato

Questa guida spiega come utilizzare la nuova funzionalità per specificare un database personalizzato durante l'importazione dei dati JSON ANAC.

## Opzioni da Riga di Comando

### Specifica Database Personalizzato

```bash
# Utilizza un database personalizzato chiamato "my_custom_db"
python -m src.import_json_mysql --database my_custom_db

# Oppure usando il file direttamente
python src/import_json_mysql.py --database my_custom_db
```

### Combinazione con Altre Opzioni

```bash
# Test di categorizzazione con database personalizzato
python -m src.import_json_mysql --test --database test_anac_db

# Pulizia tabelle con database personalizzato
python -m src.import_json_mysql --cleanup --database my_custom_db

# Importazione completa con database personalizzato
python -m src.import_json_mysql --database production_anac_2025
```

## Esempi di Utilizzo

### 1. Ambiente di Sviluppo
```bash
# Database per sviluppo
python -m src.import_json_mysql --database anac_dev
```

### 2. Ambiente di Test
```bash
# Database per test
python -m src.import_json_mysql --database anac_test --test
```

### 3. Ambiente di Produzione
```bash
# Database per produzione
python -m src.import_json_mysql --database anac_production
```

### 4. Database per Anno Specifico
```bash
# Database per dati 2024
python -m src.import_json_mysql --database anac_2024

# Database per dati 2025
python -m src.import_json_mysql --database anac_2025
```

## Configurazione Variabili d'Ambiente

Puoi anche configurare il database di default tramite variabile d'ambiente:

```bash
# Linux/Mac
export MYSQL_DATABASE=my_default_db
python -m src.import_json_mysql

# Windows
set MYSQL_DATABASE=my_default_db
python -m src.import_json_mysql
```

## Ordine di Priorità

1. **Parametro `--database`** - Massima priorità
2. **Variabile d'ambiente `MYSQL_DATABASE`** - Media priorità
3. **Valore di default** (`anac_import3`) - Minima priorità

## Note Importanti

- Il database specificato verrà **creato automaticamente** se non esiste
- Tutte le tabelle e i metadati verranno creati nel database specificato
- Assicurati che l'utente MySQL abbia i permessi per creare database
- Il cambio di database non influisce sui file JSON di origine

## Gestione degli Errori

Se il database non può essere creato o non è accessibile, il programma mostrerà un errore dettagliato e si fermerà prima dell'importazione.

## Backup e Sicurezza

Quando utilizzi database diversi, ricorda di:
- Fare backup separati per ogni database
- Configurare permessi appropriati per ogni database
- Monitorare l'utilizzo dello spazio disco 