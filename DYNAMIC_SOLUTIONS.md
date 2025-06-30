# 🚀 3 SOLUZIONI DINAMICHE per Fix Import JSON

## ❌ **PROBLEMA IDENTIFICATO**
Alcune tabelle rimangono vuote durante il build del DB perché:
1. **Path Problem**: Cerca in `/database/JSON` (assoluto) ma i file sono in `./database/JSON` (relativo)
2. **Pattern Mismatch**: I pattern regex non matchano le cartelle con struttura `YYYYMMDD-categoria_json`
3. **Empty Folders**: Alcune cartelle sono vuote e causano errori
4. **Categorizzazione Statica**: Pattern hardcoded non si adattano alla struttura reale

---

## 🎯 **SOLUZIONE 1: Auto-Discovery Dinamico con Path Fix**
### ✨ **La più robusta e adattabile**

### **Caratteristiche:**
- ✅ **Auto-discovery intelligente** del path JSON con fallback multipli
- ✅ **Pattern regex migliorati** che gestiscono timestamp e variazioni
- ✅ **Validazione file** prima dell'elaborazione
- ✅ **Logging dettagliato** per debug e monitoraggio
- ✅ **Fallback automatici** se un path non funziona

### **Implementazione:**
```python
# AUTO-DISCOVERY PATH con fallback intelligenti
def discover_json_base_path():
    """Auto-scopre il path corretto per i file JSON con fallback multipli."""
    possible_paths = [
        os.environ.get('ANAC_BASE_PATH'),  # Variabile d'ambiente se definita
        './database/JSON',                 # Path relativo standard
        'database/JSON',                   # Alternative relativo
        '/database/JSON',                  # Path assoluto fallback
        os.path.join(os.getcwd(), 'database', 'JSON')  # Path assoluto costruito
    ]
    
    for path in possible_paths:
        if path and Path(path).exists() and Path(path).is_dir():
            return str(Path(path).resolve())
    
    return './database/JSON'  # Fallback finale

# PATTERN MIGLIORATI per ogni categoria
CATEGORIES = {
    'cig': [
        r'.*cig.*',
        r'.*codice.*identificativo.*gara.*',
        r'.*smartcig.*'
    ],
    'aggiudicazioni': [
        r'.*aggiudicazioni?.*',
        r'.*aggiudicazione.*',
        r'.*aggiudicatari.*'  # Migliorato
    ],
    # ... altri pattern ottimizzati
}
```

### **Come Usare:**
```bash
# Modalità standard con auto-discovery
$env:IMPORT_MODE='standard'
python -m src.import_json_mysql
```

---

## 🧠 **SOLUZIONE 2: Categorizzazione Intelligente**
### ⚡ **Più efficiente, rileva automaticamente le strutture**

### **Caratteristiche:**
- 🔍 **Analisi contenuto** oltre ai nomi file
- 🧬 **Rilevamento schema automatico** basato sui campi JSON
- 🎯 **Recovery intelligente** per file non categorizzati
- 📊 **Statistiche avanzate** di categorizzazione
- 🚀 **Ottimizzazione automatica** per database

### **Implementazione:**
```python
def smart_categorization_with_content_analysis(base_path):
    """
    Combina pattern matching con analisi del contenuto per massima accuratezza.
    """
    # Categorizzazione base con pattern migliorati
    categories = group_files_by_category(json_files)
    
    # Analisi contenuto per file non categorizzati
    for json_file in uncategorized:
        category = analyze_file_content_for_category(json_file)
        if category:
            categories[category].append(json_file)

def analyze_file_content_for_category(json_file):
    """Analizza il contenuto di un file per determinare la categoria."""
    # Leggi primi 5 record per analisi
    fields_found = set()
    for i, line in enumerate(f):
        if i >= 5: break
        record = json.loads(line.strip())
        fields_found.update(record.keys())
    
    # Pattern di riconoscimento basati su campi
    if any(field.lower() in {'cig', 'codice_identificativo_gara'} for field in fields_found):
        return 'cig'
    elif any('aggiudicazione' in field.lower() for field in fields_found):
        return 'aggiudicazioni'
    # ... altri pattern intelligenti
```

### **Come Usare:**
```bash
# Modalità smart con categorizzazione intelligente
$env:IMPORT_MODE='smart'
python -m src.import_json_mysql
```

---

## 🌊 **SOLUZIONE 3: Streaming Incrementale con Auto-Retry**
### 💪 **La più robusta per grandi volumi**

### **Caratteristiche:**
- 🔄 **Processing incrementale** file-by-file per evitare problemi memoria
- 🛡️ **Auto-retry automatico** su errori temporanei
- 📈 **Recovery intelligente** da interruzioni
- 📊 **Tracking dettagliato** del progresso con ETA
- 🧹 **Skip automatico** di file problematici
- ⚡ **Gestione priorità** (CIG prima, poi resto)

### **Implementazione:**
```python
def streaming_incremental_import(base_path, conn):
    """
    Import streaming incrementale con auto-retry e recovery.
    """
    # Auto-discovery dei file con prioritizzazione
    categories = smart_categorization_with_content_analysis(base_path)
    
    # Prioritizza categorie per importanza
    priority_order = ['cig', 'aggiudicazioni', 'aggiudicatari', 'partecipanti']
    
    # Processa categoria per categoria con streaming
    for category, files in sorted_categories:
        for json_file in files:
            file_stats = process_single_file_streaming(
                json_file, category, schema, i, len(files)
            )

def process_single_file_streaming(json_file, category, schema, file_index, total_files):
    """Processa un singolo file con gestione errori avanzata e retry automatico."""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Process file in chunks with streaming
            chunk_size = 1000  # Chunk piccoli per streaming
            
            with open(json_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    record = json.loads(line.strip())
                    chunk.append(record)
                    
                    # Process chunk when full
                    if len(chunk) >= chunk_size:
                        success = process_chunk_with_retry(chunk, category, schema)
                        chunk = []
            
            return {'success': True, 'records_processed': records_in_file}
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
```

### **Come Usare:**
```bash
# Modalità streaming per grandi volumi
$env:IMPORT_MODE='streaming'
python -m src.import_json_mysql
```

---

## 🤖 **MODALITÀ AUTO: Selezione Automatica Intelligente**

### **Logica di Selezione:**
```python
total_files = len(json_files)
total_size_gb = sum(Path(f).stat().st_size for f in json_files) / (1024**3)

if total_files > 1000 or total_size_gb > 10:
    # Dataset GRANDE → STREAMING
elif total_files > 100 or total_size_gb > 2:
    # Dataset MEDIO → SMART  
else:
    # Dataset PICCOLO → STANDARD
```

### **Come Usare:**
```bash
# Modalità automatica (RACCOMANDATO)
$env:IMPORT_MODE='auto'  # oppure non impostare nulla
python -m src.import_json_mysql
```

---

## 🧪 **TESTING DELLE SOLUZIONI**

### **Test Rapido:**
```bash
# Testa tutte le soluzioni
python test_solutions.py --test-all

# Testa una soluzione specifica
python test_solutions.py --solution 1  # Auto-discovery
python test_solutions.py --solution 2  # Smart categorization  
python test_solutions.py --solution 3  # Streaming incrementale
```

### **Test di Categorizzazione:**
```bash
# Solo test pattern senza import
python -m src.import_json_mysql --test
```

---

## 📊 **CONFRONTO SOLUZIONI**

| Soluzione | Velocità | Memoria | Robustezza | Adattabilità | Caso d'Uso |
|-----------|----------|---------|------------|--------------|-------------|
| **Auto-Discovery** | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | **Dataset normali con problemi path** |
| **Smart Categorization** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | **File con strutture complesse** |
| **Streaming Incremental** | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | **Dataset enormi (>10GB)** |

---

## 🎯 **RACCOMANDAZIONI D'USO**

### **📋 Per la maggior parte dei casi:**
```bash
$env:IMPORT_MODE='auto'  # Selezione automatica intelligente
python -m src.import_json_mysql
```

### **🔧 Per debug problemi path:**
```bash
$env:IMPORT_MODE='standard'  # Con auto-discovery path
python -m src.import_json_mysql --test  # Prima testa
```

### **🧠 Per file complessi non categorizzati:**
```bash
$env:IMPORT_MODE='smart'  # Analisi contenuto avanzata
python -m src.import_json_mysql
```

### **🚀 Per dataset enormi (>5GB):**
```bash
$env:IMPORT_MODE='streaming'  # Processing incrementale
python -m src.import_json_mysql
```

---

## 🛠️ **VARIABILI D'AMBIENTE AVANZATE**

```bash
# Path personalizzato (priorità massima)
$env:ANAC_BASE_PATH='C:\path\to\your\JSON'

# Modalità importazione
$env:IMPORT_MODE='auto'      # auto, standard, smart, streaming

# Database personalizzato  
$env:MYSQL_DATABASE='custom_db'

# Configurazione MySQL
$env:MYSQL_HOST='localhost'
$env:MYSQL_USER='your_user'
$env:MYSQL_PASSWORD='your_password'
```

---

## 📝 **LOG E MONITORING**

### **Log Locations:**
- `logs/import_YYYYMMDD_HHMMSS.log` - Log generale importazione
- `logs/data_integrity.log` - Log integrità dati
- `logs/audit/` - Log audit e tracking

### **Indicatori di Successo:**
```
✅ [SUCCESS] File processsato: filename.json (1,234 record)
🎯 [SMART-RESULT] Trovate 15 categorie intelligenti
📊 [AUTO-CHOICE] Dataset MEDIO (245 file, 3.2GB) → SMART
🎉 [COMPLETE] Importazione completata!
```

### **Troubleshooting:**
```
❌ [ERROR] Nessun file JSON trovato
⚠️ [WARNING] Fallback a modalità standard  
🔄 [RETRY] Tentativo 2/3 per filename.json
```

---

## 🆘 **SUPPORTO E DEBUG**

### **Se le tabelle sono ancora vuote:**
1. **Verifica path JSON:**
   ```bash
   python test_solutions.py --solution 1
   ```

2. **Testa categorizzazione:**
   ```bash
   python -m src.import_json_mysql --test
   ```

3. **Forza modalità streaming:**
   ```bash
   $env:IMPORT_MODE='streaming'
   python -m src.import_json_mysql
   ```

### **Per assistenza avanzata:**
- Controlla `logs/import_*.log` per errori dettagliati
- Usa `--test` prima dell'import vero
- Prova le diverse modalità in ordine: auto → smart → streaming 