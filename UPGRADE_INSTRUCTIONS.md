# 🚀 Istruzioni di Aggiornamento Sistema Linux

## ⚠️ IMPORTANTE: Aggiornamento Richiesto

Il sistema ha ricevuto aggiornamenti importanti per supportare database personalizzati e correzioni di bug critici.

## 📋 Passi da seguire sul sistema Linux:

### 1. **Aggiorna il repository**
```bash
cd /database/DBmake
git stash  # Salva eventuali modifiche locali
git pull origin MULTITAB
git stash pop  # Ripristina le modifiche locali se necessario
```

### 2. **Verificar i nuovi file**
```bash
# Controlla che questi file siano aggiornati:
ls -la run.sh  # Dovrebbe avere timestamp recente
ls -la src/mysql_import_wrapper.py  # Dovrebbe supportare --database
ls -la src/database/config.py  # Nuovo file per configurazione
```

### 3. **Test delle nuove funzionalità**

#### Test database personalizzato con wrapper:
```bash
python src/mysql_import_wrapper.py --help
# Dovrebbe mostrare: --database DATABASE

python src/mysql_import_wrapper.py --database test_db_2025
```

#### Test database personalizzato con script principale:
```bash
python -m src.import_json_mysql --help
# Dovrebbe mostrare: --database DATABASE

python -m src.import_json_mysql --test --database test_categories
```

### 4. **Utilizzo del menu interattivo aggiornato**
```bash
./run.sh
# Scegli opzione 3 per "Auto-Turbo MySQL Import"
# Ora dovrebbe apparire un submenu per scegliere il database
```

## 🎯 Nuove funzionalità disponibili:

### **Database personalizzato tramite parametri**
```bash
# Wrapper MySQL con database personalizzato
python src/mysql_import_wrapper.py --database anac_2025

# Script principale con database personalizzato
python -m src.import_json_mysql --database anac_production
python -m src.import_json_mysql --test --database test_env
python -m src.import_json_mysql --cleanup --database old_data
```

### **Menu interattivo migliorato**
- Ora quando scegli l'opzione 3 nel menu principale
- Ti chiederà se vuoi usare il database di default o specificarne uno personalizzato
- Supporta creazione automatica di database se non esistono

### **Ordine di priorità configurazione database:**
1. **Parametro `--database`** (massima priorità)
2. **Variabile d'ambiente `MYSQL_DATABASE`** (media priorità)
3. **Default `anac_import3`** (minima priorità)

## 🔧 Correzioni di bug incluse:

- ✅ **Fix DatabaseManager cursor error** - Risolto errore `'DatabaseManager' object has no attribute 'cursor'`
- ✅ **Import scope fix** - Corretto problema di import del DatabaseManager
- ✅ **Configurazione dinamica** - Sistema robusto per cambio database runtime
- ✅ **Menu interattivo** - Migliorata UX per selezione database

## 🚨 Se il menu non funziona ancora:

Se dopo l'aggiornamento il menu non mostra le opzioni per il database:

```bash
# Forza refresh del file run.sh
git checkout MULTITAB -- run.sh
chmod +x run.sh

# Oppure usa direttamente i comandi:
python src/mysql_import_wrapper.py --database your_custom_db
```

## 📞 Support

Se hai problemi durante l'aggiornamento:
1. Verifica di essere sul branch MULTITAB: `git branch`
2. Controlla i log di git: `git log --oneline -5`
3. Verifica i permessi: `chmod +x run.sh` 