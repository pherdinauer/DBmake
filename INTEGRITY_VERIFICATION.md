# Sistema di Verifica dell'Integrità dei Dati

## Panoramica

Il sistema di verifica dell'integrità dei dati confronta automaticamente i record presenti nei file JSON di origine con quelli effettivamente inseriti nel database MySQL, fornendo report dettagliati su eventuali discrepanze.

## Caratteristiche Principali

### ✅ **Verifica Completa**
- Conta i record nei file JSON originali
- Conta i record nel database MySQL
- Identifica record mancanti o non inseriti
- Calcola tassi di successo per file e globali

### 📊 **Reporting Avanzato**
- Report JSON strutturati con timestamp
- Log dettagliati per ogni file elaborato
- File separati per i dati mancanti
- Statistiche globali e per singolo file

### 🔍 **Analisi Dettagliata**
- Hash SHA256 per integrità dei file
- Dimensioni file e tempi di elaborazione
- Identificazione specifica dei record mancanti
- Gestione errori con logging dettagliato

### 🛡️ **Robustezza**
- Gestione errori resiliente
- Configurazione database flessibile
- Supporto per diversi formati JSON
- Logging multi-livello

## Installazione e Setup

### Prerequisiti
```bash
# Installa le dipendenze Python necessarie
pip install mysql-connector-python
```

### Configurazione Database
Configura le variabili d'ambiente (opzionale):
```bash
export MYSQL_HOST=localhost
export MYSQL_USER=Nando
export MYSQL_PASSWORD=your_password
export MYSQL_DATABASE=anac_import3
```

## Utilizzo

### Metodo 1: Script Semplice
```bash
# Verifica con database di default
python check_data_integrity.py

# Verifica con database personalizzato
python check_data_integrity.py --database anac_produzione

# Verifica con output verboso
python check_data_integrity.py --database anac_test --verbose
```

### Metodo 2: Uso Diretto del Modulo
```python
from src.data_integrity_checker import DataIntegrityChecker

# Configurazione database
db_config = {
    'host': 'localhost',
    'user': 'Nando', 
    'password': '',
    'database': 'anac_import3',
    'charset': 'utf8mb4'
}

# Crea checker e esegui verifica
checker = DataIntegrityChecker(db_config)
report = checker.run_full_integrity_check()

# Accedi ai risultati
print(f"Record mancanti: {report.total_missing_records}")
print(f"Tasso successo: {report.global_success_rate:.2f}%")
```

## Output e Report

### 📁 **Struttura File di Output**
```
logs/
├── data_integrity.log                    # Log principale
├── integrity_report_YYYYMMDD_HHMMSS.json # Report JSON completo
├── detailed_integrity_log_YYYYMMDD_HHMMSS.txt # Log dettagliato
└── missing_data/
    ├── missing_file1_YYYYMMDD_HHMMSS.json
    ├── missing_file2_YYYYMMDD_HHMMSS.json
    └── ...
```

### 📊 **Esempio Report Console**
```
================================================================================
📊 RIEPILOGO VERIFICA INTEGRITÀ DATI
================================================================================
📁 File elaborati: 15
📄 Record JSON totali: 125,847
🗄️ Record database totali: 125,847
❌ Record mancanti: 0
✅ Tasso successo globale: 100.00%

⏱️ Durata elaborazione: 45.3 secondi
================================================================================
```

### 📋 **Formato Report JSON**
```json
{
  "total_files_processed": 15,
  "total_source_records": 125847,
  "total_database_records": 125847,
  "total_missing_records": 0,
  "global_success_rate": 100.0,
  "processing_start": "2025-06-23T15:30:00",
  "processing_end": "2025-06-23T15:30:45",
  "files_with_issues": [],
  "detailed_reports": [
    {
      "filename": "aggiudicazioni_2024.json",
      "source_records": 8547,
      "database_records": 8547,
      "missing_records": 0,
      "success_rate": 100.0,
      "processing_time": "0:00:03.245",
      "file_size_mb": 12.5,
      "file_hash": "abc123...",
      "timestamp": "2025-06-23T15:30:00",
      "errors": [],
      "missing_data_details": []
    }
  ]
}
```

### 📄 **File Dati Mancanti**
```json
{
  "filename": "example_file.json",
  "timestamp": "20250623_153000",
  "missing_count": 5,
  "missing_records": [
    {
      "cig": "Z1234567890",
      "descrizione": "Servizio mancante",
      "importo": 50000
    }
  ]
}
```

## Configurazione Avanzata

### 🎛️ **Parametri Personalizzabili**
```python
# Nel file data_integrity_checker.py, puoi modificare:

# Percorsi dei dati JSON
self.json_data_paths = {
    'aggiudicazioni': 'data/downloads/aggiudicazioni',
    'partecipanti': 'data/downloads/partecipanti',
    # ... aggiungi altri percorsi
}

# Mapping file -> tabelle database
def get_table_name_from_file(self, filename: str, dataset_type: str) -> str:
    # Personalizza la logica di mapping
```

### 🔧 **Configurazioni Database Multiple**
```python
# Database di produzione
prod_config = {
    'host': 'prod-server.com',
    'database': 'anac_produzione'
}

# Database di test
test_config = {
    'host': 'localhost', 
    'database': 'anac_test'
}
```

## Integrazione con Altri Sistemi

### 🔄 **Automazione con Cron**
```bash
# Aggiungi al crontab per verifica giornaliera
0 6 * * * cd /path/to/DBmake && python check_data_integrity.py --database anac_produzione > /var/log/integrity_check.log 2>&1
```

### 🚨 **Integrazione con Sistema di Alerting**
```python
# Esempio: Invio email se ci sono problemi
def send_alert_if_issues(report):
    if report.total_missing_records > 0:
        send_email(
            subject=f"⚠️ Integrità Dati: {report.total_missing_records} record mancanti",
            body=f"Verifica completata con {len(report.files_with_issues)} file problematici"
        )
```

### 📈 **Integrazione con CI/CD**
```yaml
# GitHub Actions esempio
- name: Check Data Integrity
  run: |
    python check_data_integrity.py --database test_db
    if [ $? -ne 0 ]; then
      echo "❌ Test di integrità fallito"
      exit 1
    fi
```

## Risoluzione Problemi Comuni

### 🔧 **Errori di Connessione Database**
```bash
# Verifica configurazione
mysql -h localhost -u Nando -p anac_import3 -e "SELECT 1"

# Controlla variabili ambiente
echo $MYSQL_HOST $MYSQL_USER $MYSQL_DATABASE
```

### 📁 **File JSON Non Trovati**
```bash
# Verifica che i percorsi siano corretti
ls -la data/downloads/aggiudicazioni/*.json

# Controlla permessi
chmod 644 data/downloads/*/*.json
```

### 🐛 **Errori di Mappatura Tabelle**
- Controlla che i nomi delle tabelle nel database corrispondano al mapping
- Verifica che la primary key 'cig' esista nelle tabelle
- Assicurati che le tabelle siano state create correttamente

### 💾 **Problemi di Memoria con File Grandi**
```python
# Per file molto grandi, modifica la strategia di lettura
def count_json_records_streaming(self, file_path: Path):
    # Implementa lettura in streaming per file > 1GB
    pass
```

## Best Practices

### ✅ **Esecuzione Regolare**
- Esegui verifica dopo ogni import
- Pianifica controlli giornalieri automatici
- Monitora i trend nei tassi di successo

### 📊 **Analisi dei Report**
- Conserva i report storici per analisi trend
- Monitora file che spesso hanno problemi
- Analizza i pattern nei dati mancanti

### 🔒 **Sicurezza**
- Non includere password in script version-controlled
- Usa variabili d'ambiente per credenziali sensibili
- Limita l'accesso ai file di log con dati sensibili

### ⚡ **Performance**
- Esegui verifiche in orari di basso carico
- Considera l'indicizzazione per chiavi di verifica
- Monitora l'uso di memoria per file grandi

## Estensioni Future

### 🚀 **Funzionalità Pianificate**
- Verifica cross-reference tra tabelle correlate
- Validazione schema JSON con JSON Schema
- Verifica integrità referenziale (foreign keys)
- Dashboard web per visualizzazione report
- API REST per integrazione con altri sistemi

### 🛠️ **Personalizzazioni Possibili**
- Soglie personalizzabili per alerting
- Verifica di integrità dei dati (hash, checksums)
- Supporto per altri database (PostgreSQL, Oracle)
- Integrazione con sistemi di monitoring (Prometheus, Grafana)

---

## Supporto

Per problemi, domande o suggerimenti:
1. Controlla i log in `logs/data_integrity.log`
2. Verifica la configurazione database
3. Controlla che tutti i file JSON siano accessibili
4. Consulta questa documentazione per risoluzione problemi comuni

---

**📚 Documentazione aggiornata al:** 23 Giugno 2025  
**🔧 Versione sistema:** 1.0.0 