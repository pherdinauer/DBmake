# 🧙‍♂️ ANAC Importer Enterprise - Wizard CLI Guide

## Panoramica

Il **Setup Wizard Interattivo** è la nuova interfaccia user-friendly per configurare e gestire il sistema ANAC Importer Enterprise. Progettato per guidarti passo dopo passo attraverso tutto il processo di configurazione.

## 🚀 Avvio Rapido

### Metodo 1: Script Semplificato (Raccomandato)
```bash
# Primo setup completo
./anac-enterprise setup

# Uso quotidiano
./anac-enterprise import file.json
./anac-enterprise status
./anac-enterprise test
```

### Metodo 2: CLI Diretto
```bash
# Setup wizard
python3 src/main_enterprise.py setup-wizard

# Altri comandi
python3 src/main_enterprise.py --help
```

## 🔧 Setup Wizard Completo

### Caratteristiche del Wizard

Il wizard ti guida attraverso **6 step** fondamentali:

1. **📋 Configurazione Credenziali Database**
   - Host MySQL (default: localhost)
   - Porta MySQL (default: 3306)
   - Username e password con validazione sicurezza
   - Nome database con creazione automatica
   - Configurazione SSL

2. **🔗 Test Connessione Database**
   - Verifica connettività server MySQL
   - Controllo permessi utente
   - Suggerimenti automatici per errori comuni

3. **🗄️ Setup Database**
   - Verifica esistenza database
   - Creazione automatica se necessario
   - Gestione database esistenti

4. **🏗️ Inizializzazione Schema Enterprise**
   - Creazione tabelle enterprise
   - Verifica integrità schema
   - Report dettagliato delle operazioni

5. **💾 Salvataggio Credenziali Sicure**
   - **Keyring di sistema** (raccomandato)
   - **Variabili d'ambiente**
   - **File .env** (meno sicuro)

6. **🔍 Validazione Finale**
   - Test connessione completa
   - Test inserimento e lettura dati
   - Verifica integrità end-to-end

### Esempio di Utilizzo del Wizard

```bash
$ ./anac-enterprise setup

🚀 ANAC Importer Enterprise Edition
Setup Wizard Interattivo

Questo wizard ti guiderà attraverso la configurazione completa del sistema:
• Configurazione credenziali database sicure
• Test connettività e permessi
• Creazione database automatica (se necessario)
• Inizializzazione schema enterprise
• Validazione setup completo

Vuoi procedere con il setup? [Y/n]: Y

📋 STEP 1: Configurazione Credenziali Database

🌐 Host MySQL [localhost]: 
🔌 Porta MySQL [3306]: 
👤 Username MySQL: myuser
🔑 Password MySQL: ********
✅ Password sicura
🗄️ Nome database ANAC [anac_enterprise]: 
🔒 Abilitare SSL per connessioni sicure? [Y/n]: Y

📋 Riepilogo Credenziali
┌──────────┬─────────────────┐
│ Parametro│ Valore          │
├──────────┼─────────────────┤
│ Host     │ localhost       │
│ Porta    │ 3306            │
│ Username │ myuser          │
│ Password │ ••••••••        │
│ Database │ anac_enterprise │
│ SSL      │ Abilitato       │
└──────────┴─────────────────┘

✅ Confermi le credenziali? [Y/n]: Y

🔗 STEP 2: Test Connessione Database
⠋ 🌐 Connessione al server MySQL...
✅ Connesso al server MySQL
⠋ 🔑 Verifica permessi utente...
✅ Permessi sufficienti
✅ Connessione al server MySQL riuscita!

🗄️ STEP 3: Setup Database
⠋ 🔍 Verifica esistenza database...
✅ Database esistente trovato
✅ Database 'anac_enterprise' già esistente

🏗️ STEP 4: Inizializzazione Schema Enterprise
⠋ 🔌 Connessione al database...
✅ Connesso al database
⠋ 🏗️ Creazione tabelle enterprise...
✅ Schema enterprise inizializzato
⠋ 🔍 Verifica integrità schema...
✅ Integrità schema verificata

📊 Informazioni Schema Enterprise
┌────────────────────┬─────────┐
│ Proprietà          │ Valore  │
├────────────────────┼─────────┤
│ Versione Schema    │ 1.0.0   │
│ Numero Tabelle     │ 12      │
│ Ultimo Aggiornamento│ 2024... │
│ Schema Manager     │ 1.0.0   │
└────────────────────┴─────────┘

💾 STEP 5: Salvataggio Credenziali Sicure

Come vuoi salvare le credenziali?
[1] Keyring di sistema (raccomandato)
[2] Variabili d'ambiente
[3] File .env (meno sicuro)
Scegli [1-3]: 1

✅ Credenziali salvate nel keyring di sistema

🔍 STEP 6: Validazione Finale
⠋ 🔗 Test connessione completa...
✅ Connessione completa OK
⠋ 📝 Test inserimento dati...
✅ Test inserimento OK
⠋ 📖 Test lettura dati...
✅ Test lettura OK

┌────────────────────────────────────────────────────────────┐
│                    🎊 Setup Completato                    │
├────────────────────────────────────────────────────────────┤
│                                                            │
│ 🎉 SETUP COMPLETATO CON SUCCESSO!                         │
│                                                            │
│ Il tuo sistema ANAC Importer Enterprise è ora             │
│ completamente configurato:                                 │
│                                                            │
│ ✅ Database configurato e pronto                          │
│ ✅ Schema enterprise inizializzato                        │
│ ✅ Credenziali salvate in modo sicuro                     │
│ ✅ Connettività verificata                                │
│ ✅ Sistema pronto per l'uso                               │
│                                                            │
│ PROSSIMI PASSI:                                            │
│                                                            │
│ 1. Testare l'importazione:                                 │
│    python3 src/main_enterprise.py import-files file.json  │
│                                                            │
│ 2. Verificare configurazione:                              │
│    python3 src/main_enterprise.py test-connection         │
│                                                            │
│ 3. Consultare la documentazione:                           │
│    cat IMPLEMENTAZIONE_COMPLETA_ENTERPRISE.md             │
│                                                            │
│ Buon lavoro con ANAC Importer Enterprise! 🚀              │
└────────────────────────────────────────────────────────────┘

✅ Setup completato con successo!
```

## 🎯 Comandi CLI Disponibili

### Script Semplificato (`./anac-enterprise`)

```bash
# Setup iniziale
./anac-enterprise setup

# Importazione file
./anac-enterprise import file.json
./anac-enterprise import file1.json file2.json file3.json

# Monitoraggio sistema
./anac-enterprise status
./anac-enterprise test
./anac-enterprise jobs

# Aiuto e guide
./anac-enterprise help
./anac-enterprise quickstart

# Modalità avanzata
./anac-enterprise advanced import-files --validation enterprise file.json
./anac-enterprise advanced --help
```

### CLI Completo

```bash
# Wizard e setup
python3 src/main_enterprise.py setup-wizard
python3 src/main_enterprise.py setup-credentials

# Importazione
python3 src/main_enterprise.py import-files file.json
python3 src/main_enterprise.py import-files --validation enterprise --batch-size 2000 file.json

# Monitoraggio
python3 src/main_enterprise.py status
python3 src/main_enterprise.py test-connection
python3 src/main_enterprise.py list-jobs
python3 src/main_enterprise.py list-jobs --format json

# Aiuto
python3 src/main_enterprise.py --help
python3 src/main_enterprise.py quickstart
```

## 🔒 Opzioni di Salvataggio Credenziali

### 1. Keyring di Sistema (Raccomandato)
- **Sicurezza massima**: Usa il keyring del sistema operativo
- **Trasparente**: Credenziali gestite automaticamente
- **Multipiattaforma**: Windows, macOS, Linux
- **Accesso**: Automatico quando necessario

### 2. Variabili d'Ambiente
- **Portabilità**: Facilmente configurabile in container/CI
- **Sicurezza media**: Visibili nel processo
- **Setup manuale**: Devi impostare tu le variabili

```bash
export MYSQL_HOST="localhost"
export MYSQL_PORT="3306"
export MYSQL_USER="myuser"
export MYSQL_PASSWORD="mypassword"
export MYSQL_DATABASE="anac_enterprise"
export MYSQL_SSL_DISABLED="false"
```

### 3. File .env
- **Sicurezza minima**: File in plain text
- **Convenienza**: Facile da modificare
- **Rischio**: Può essere committato per errore

```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=myuser
MYSQL_PASSWORD=mypassword
MYSQL_DATABASE=anac_enterprise
MYSQL_SSL_DISABLED=false
```

## 📊 Comando Status Dettagliato

Il comando `status` fornisce una panoramica completa del sistema:

```bash
$ ./anac-enterprise status

📊 ANAC Importer Enterprise - Status Sistema
==================================================

📦 MODULI:
   ✅ Security Module: OK
   ✅ Database Module: OK
   ✅ Core Module: OK

🔑 CREDENZIALI:
   ✅ Credenziali: Disponibili
   🌐 Host: localhost
   👤 User: myuser
   🗄️ Database: anac_enterprise

🔗 CONNESSIONE DATABASE:
   ✅ Connessione: OK
   📊 Schema Version: 1.0.0
   📋 Tabelle: 12

📁 DIRECTORIES:
   ✅ logs/: Presente
   ✅ database/: Presente
   ✅ demo/: Presente

==================================================
```

## 🚀 Flusso di Lavoro Tipico

### Prima Installazione
```bash
# 1. Clona e setup
git clone <repository>
cd anac-importer
pip install -r requirements.txt

# 2. Setup completo guidato
./anac-enterprise setup

# 3. Test sistema
./anac-enterprise test

# 4. Prima importazione
./anac-enterprise import demo/sample_data.json
```

### Uso Quotidiano
```bash
# Verifica sistema
./anac-enterprise status

# Importa dati
./anac-enterprise import data/nuovo_file.json

# Monitora job
./anac-enterprise jobs

# In caso di problemi
./anac-enterprise test
```

## 🔍 Risoluzione Problemi

### Errori Comuni

**"Wizard non disponibile"**
```bash
# Installa dipendenze mancanti
pip install rich click mysql-connector-python
```

**"Connessione database fallita"**
- Verifica che MySQL sia in esecuzione
- Controlla host, porta, username, password
- Verifica permessi dell'utente

**"Schema non inizializzato"**
```bash
# Riavvia il wizard
./anac-enterprise setup
```

**"Credenziali non trovate"**
```bash
# Riconfigura credenziali
./anac-enterprise setup
```

### Log e Debug

```bash
# Output verboso
./anac-enterprise advanced import-files --verbose file.json

# Controlla log
tail -f logs/anac_enterprise_*.log

# Status dettagliato
./anac-enterprise status
```

## 🎯 Best Practices

1. **Primo Setup**: Usa sempre il wizard per la configurazione iniziale
2. **Credenziali**: Preferisci sempre il keyring di sistema
3. **Test**: Esegui `status` e `test` prima delle importazioni
4. **Monitoraggio**: Controlla regolarmente i job con `jobs`
5. **Backup**: Fai backup del database prima di importazioni massicce
6. **Sicurezza**: Non committare mai file .env in git

## 📚 Documentazione Correlata

- [Implementazione Enterprise Completa](IMPLEMENTAZIONE_COMPLETA_ENTERPRISE.md)
- [Analisi e Piano Miglioramento](ANALISI_APPROFONDITA_E_PIANO_MIGLIORAMENTO.md)
- [Demo Sistema](demo/enterprise_demo.py)

---

🎉 **Il wizard CLI rende l'uso di ANAC Importer Enterprise semplice e sicuro!**