# 🚀 GUIDA RAPIDA - CLI UNIFICATO per Import JSON ANAC

## 🎯 **LA MIGLIORE SOLUZIONE PER IL TUO CASO**

Basandomi sulla tua struttura file JSON:
```
20240201-aggiudicatari_json/20240201-aggiudicatari_json.json
20240401-aggiudicazioni_json/20240401-aggiudicazioni_json.json
```

**🧠 SMART MODE è la MIGLIORE** perché:
- ✅ Analizza **contenuto dei file** oltre ai nomi
- ✅ Gestisce **timestamp complessi** (`YYYYMMDD-`)
- ✅ **Recovery automatico** per file non categorizzati
- ✅ **Velocità ottimale** per la tua dimensione dataset

---

## 📋 **COMANDI ESSENZIALI (quello che usi davvero)**

### **🧪 1. TEST PRIMA (SEMPRE)**
```bash
python run_import.py --test
```
**Cosa fa:** Verifica che trovi e categorizzi correttamente i tuoi file JSON

### **🚀 2. IMPORTAZIONE PRINCIPALE**
```bash
python run_import.py --run
```
**Cosa fa:** Importazione diretta con SMART mode (migliore per te)

### **🔧 3. FIX TABELLE VUOTE**
```bash
python run_import.py --fix-empty
```
**Cosa fa:** Fix specifico per il problema delle tabelle vuote

---

## 🔍 **WORKFLOW COMPLETO RACCOMANDATO**

### **Step 1: Verifica Stato**
```bash
python run_import.py --status
```

### **Step 2: Test Categorizzazione**
```bash
python run_import.py --test
```
**✅ Se ok:** Vai al Step 3  
**❌ Se problemi:** Risolvi path/file JSON

### **Step 3: Importazione**
```bash
python run_import.py --run
```

### **Step 4: Verifica Log**
```bash
python run_import.py --logs
```

---

## 🛠️ **COMANDI AVANZATI**

| Comando | Uso | Quando |
|---------|-----|--------|
| `python run_import.py --smart` | Modalità SMART esplicita | Per controllo manuale |
| `python run_import.py --streaming` | Streaming incrementale | Dataset >10GB |
| `python run_import.py --auto` | Auto-detect modalità | Quando non sei sicuro |
| `python run_import.py --test-all` | Test tutte le soluzioni | Per debug avanzato |

---

## ⚙️ **OPZIONI PERSONALIZZAZIONE**

### **Path JSON Personalizzato:**
```bash
python run_import.py --path "/path/to/your/json" --run
```

### **Database Personalizzato:**
```bash
python run_import.py --database "custom_db" --run
```

### **Modalità Automatica (no conferme):**
```bash
python run_import.py --run --force
```

---

## 🆘 **TROUBLESHOOTING**

### **❌ Tabelle ancora vuote?**
```bash
python run_import.py --fix-empty
```

### **❌ File non trovati?**
```bash
python run_import.py --status
# Controlla il path mostrato
```

### **❌ Errori durante import?**
```bash
python run_import.py --logs
# Controlla gli ultimi log
```

### **❌ Categorizzazione non funziona?**
```bash
python run_import.py --test-all
# Testa tutte le soluzioni
```

---

## 🎯 **RACCOMANDAZIONE FINALE**

**Per la tua struttura specifica** (timestamp + categorie multiple):

### **🏃‍♂️ QUICK START (2 comandi):**
```bash
# 1. Test
python run_import.py --test

# 2. Import se test ok
python run_import.py --run
```

### **🔧 PROBLEMI TABELLE VUOTE:**
```bash
python run_import.py --fix-empty
```

**Il CLI gestisce tutto automaticamente!** 🎉

---

## 📝 **LOG E MONITORING**

- **Log automatici in:** `logs/import_*.log`
- **Comando per vedere log:** `python run_import.py --logs`
- **Status sistema:** `python run_import.py --status`

---

## 💡 **TIPS FINALI**

1. **SEMPRE** fai `--test` prima di `--run`
2. Usa `--fix-empty` se le tabelle sono vuote
3. `--status` ti mostra tutto quello che serve sapere
4. `--logs` per vedere cosa è successo
5. Il default è già SMART mode (migliore per te)

**🎯 TL;DR: `python run_import.py --test` poi `python run_import.py --run`** 