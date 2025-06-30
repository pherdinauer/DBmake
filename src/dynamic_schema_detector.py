"""
SISTEMA DINAMICO di RILEVAMENTO SCHEMA per JSON ANAC

Rileva automaticamente:
1. Strutture dei file JSON
2. Categorie basate su contenuto (non solo nomi)
3. Campi comuni tra file simili
4. Ottimizzazioni per database
"""

import os
import json
import re
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple, Any
import logging
from datetime import datetime

# Setup logging
logger = logging.getLogger(__name__)

class DynamicSchemaDetector:
    """Rileva dinamicamente schemi e categorie dai file JSON."""
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.file_analysis = {}
        self.categories = defaultdict(list)
        self.schema_cache = {}
        
    def analyze_all_files(self) -> Dict[str, Any]:
        """
        Analizza tutti i file JSON e rileva automaticamente categorie e schemi.
        
        Returns:
            Dict con analisi completa: {
                'categories': {...},
                'schemas': {...},
                'statistics': {...}
            }
        """
        logger.info("🔍 [DYNAMIC] Avvio analisi dinamica file JSON...")
        
        # 1. Scopri tutti i file JSON validi
        json_files = self._discover_json_files()
        
        # 2. Analizza struttura di ogni file
        for json_file in json_files:
            self._analyze_single_file(json_file)
        
        # 3. Raggruppa per similarità di schema
        self._group_by_schema_similarity()
        
        # 4. Genera categorie ottimizzate
        optimized_categories = self._generate_optimized_categories()
        
        # 5. Crea schemi per categoria
        category_schemas = self._create_category_schemas()
        
        return {
            'categories': optimized_categories,
            'schemas': category_schemas,
            'statistics': self._generate_statistics(),
            'file_analysis': self.file_analysis
        }
    
    def _discover_json_files(self) -> List[Path]:
        """Scopre ricorsivamente tutti i file JSON validi."""
        json_files = []
        
        for json_file in self.base_path.rglob("*.json"):
            if self._is_valid_json_file(json_file):
                json_files.append(json_file)
        
        logger.info(f"📂 [DISCOVERY] {len(json_files)} file JSON validi trovati")
        return json_files
    
    def _is_valid_json_file(self, file_path: Path) -> bool:
        """Verifica rapidamente se un file JSON è valido."""
        try:
            if file_path.stat().st_size == 0:
                return False
            
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                return first_line.startswith(('{', '['))
        except:
            return False
    
    def _analyze_single_file(self, file_path: Path) -> None:
        """Analizza la struttura di un singolo file JSON."""
        try:
            analysis = {
                'path': str(file_path),
                'name': file_path.name,
                'parent': file_path.parent.name,
                'size': file_path.stat().st_size,
                'fields': set(),
                'sample_record': None,
                'record_count': 0,
                'field_types': defaultdict(set),
                'category_hints': []
            }
            
            # Leggi campioni di record per analisi
            with open(file_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 100:  # Analizza max 100 record per file
                        break
                    
                    try:
                        record = json.loads(line.strip())
                        analysis['record_count'] = i + 1
                        
                        if analysis['sample_record'] is None:
                            analysis['sample_record'] = record
                        
                        # Analizza campi
                        for field, value in record.items():
                            analysis['fields'].add(field.lower())
                            analysis['field_types'][field.lower()].add(type(value).__name__)
                        
                    except json.JSONDecodeError:
                        continue
            
            # Rileva categoria basata su contenuto
            analysis['category_hints'] = self._detect_category_from_content(analysis)
            
            self.file_analysis[str(file_path)] = analysis
            
        except Exception as e:
            logger.warning(f"⚠️  [ANALYSIS] Errore analisi {file_path}: {e}")
    
    def _detect_category_from_content(self, analysis: Dict) -> List[str]:
        """Rileva categoria basandosi sui campi presenti."""
        fields = analysis['fields']
        hints = []
        
        # Pattern di riconoscimento basati su campi caratteristici
        category_patterns = {
            'cig': {'cig', 'codice_identificativo_gara'},
            'aggiudicazioni': {'importo_aggiudicazione', 'aggiudicatario'},
            'aggiudicatari': {'codice_fiscale', 'partita_iva', 'ragione_sociale'},
            'partecipanti': {'partecipanti', 'codice_fiscale_partecipante'},
            'pubblicazioni': {'data_pubblicazione', 'url_pubblicazione'},
            'varianti': {'importo_variante', 'data_variante'},
            'subappalti': {'importo_subappalto', 'subappaltatore'},
            'collaudo': {'data_collaudo', 'esito_collaudo'},
            'stati_avanzamento': {'percentuale_avanzamento', 'data_stato'},
            'lavorazioni': {'tipo_lavorazione', 'categoria_opera'},
            'sospensioni': {'data_sospensione', 'motivo_sospensione'},
            'quadro_economico': {'importo_base', 'importo_appalto'},
            'fonti_finanziamento': {'tipo_finanziamento', 'importo_finanziamento'}
        }
        
        for category, required_fields in category_patterns.items():
            # Se almeno il 50% dei campi caratteristici è presente
            overlap = len(required_fields.intersection(fields))
            if overlap >= len(required_fields) * 0.5:
                hints.append((category, overlap / len(required_fields)))
        
        # Ordina per confidenza
        hints.sort(key=lambda x: x[1], reverse=True)
        return [hint[0] for hint in hints]
    
    def _group_by_schema_similarity(self) -> None:
        """Raggruppa file con schemi simili."""
        for file_path, analysis in self.file_analysis.items():
            # Usa il primo hint di categoria se disponibile
            if analysis['category_hints']:
                category = analysis['category_hints'][0]
            else:
                # Fallback: usa pattern nel nome
                category = self._extract_category_from_name(analysis['name'])
            
            self.categories[category].append(file_path)
    
    def _extract_category_from_name(self, filename: str) -> str:
        """Estrae categoria dal nome file come fallback."""
        # Pulisci il nome
        clean_name = re.sub(r'^\d{8}[-_]', '', filename)
        clean_name = re.sub(r'[_-]json\.json$', '', clean_name)
        clean_name = re.sub(r'_\d{4}_\d{2}$', '', clean_name)
        
        # Pattern comuni
        if 'aggiudicatar' in clean_name:
            return 'aggiudicatari'
        elif 'aggiudicazion' in clean_name:
            return 'aggiudicazioni'
        elif 'cig' in clean_name or 'smartcig' in clean_name:
            return 'cig'
        elif 'partecipant' in clean_name:
            return 'partecipanti'
        elif 'pubblicazion' in clean_name:
            return 'pubblicazioni'
        else:
            return 'unknown'
    
    def _generate_optimized_categories(self) -> Dict[str, List[str]]:
        """Genera categorie ottimizzate rimuovendo categorie vuote."""
        optimized = {}
        
        for category, files in self.categories.items():
            if files and category != 'unknown':
                optimized[category] = files
                logger.info(f"📊 [CATEGORY] {category}: {len(files)} file")
        
        return optimized
    
    def _create_category_schemas(self) -> Dict[str, Dict]:
        """Crea schemi ottimizzati per ogni categoria."""
        schemas = {}
        
        for category, files in self.categories.items():
            if not files:
                continue
            
            # Unisci tutti i campi della categoria
            all_fields = set()
            field_types = defaultdict(set)
            
            for file_path in files:
                if file_path in self.file_analysis:
                    analysis = self.file_analysis[file_path]
                    all_fields.update(analysis['fields'])
                    
                    for field, types in analysis['field_types'].items():
                        field_types[field].update(types)
            
            # Genera schema MySQL ottimizzato
            mysql_schema = {}
            for field in all_fields:
                types = field_types[field]
                mysql_type = self._determine_mysql_type(field, types)
                mysql_schema[field] = mysql_type
            
            schemas[category] = {
                'fields': list(all_fields),
                'mysql_schema': mysql_schema,
                'file_count': len(files)
            }
        
        return schemas
    
    def _determine_mysql_type(self, field: str, python_types: Set[str]) -> str:
        """Determina il tipo MySQL ottimale per un campo."""
        # Se contiene solo int
        if python_types == {'int'}:
            return 'BIGINT'
        
        # Se contiene float
        if 'float' in python_types:
            return 'DECIMAL(15,2)'
        
        # Se contiene bool
        if 'bool' in python_types:
            return 'BOOLEAN'
        
        # Campi speciali
        if 'cig' in field.lower():
            return 'VARCHAR(15)'
        elif 'fiscale' in field.lower():
            return 'VARCHAR(16)'
        elif 'data' in field.lower():
            return 'DATE'
        elif 'importo' in field.lower():
            return 'DECIMAL(15,2)'
        
        # Default: VARCHAR con lunghezza adattiva
        return 'VARCHAR(255)'
    
    def _generate_statistics(self) -> Dict[str, Any]:
        """Genera statistiche dettagliate dell'analisi."""
        total_files = len(self.file_analysis)
        total_categories = len(self.categories)
        
        category_stats = {}
        for category, files in self.categories.items():
            category_stats[category] = {
                'file_count': len(files),
                'total_size': sum(
                    self.file_analysis.get(f, {}).get('size', 0) 
                    for f in files
                )
            }
        
        return {
            'total_files_analyzed': total_files,
            'total_categories_found': total_categories,
            'category_breakdown': category_stats,
            'analysis_timestamp': datetime.now().isoformat()
        }

def create_dynamic_import_plan(base_path: str) -> Dict[str, Any]:
    """
    Crea un piano di importazione dinamico basato sull'analisi dei file.
    
    Returns:
        Piano di importazione ottimizzato con categorie e schemi automatici.
    """
    detector = DynamicSchemaDetector(base_path)
    analysis_result = detector.analyze_all_files()
    
    logger.info("🎯 [DYNAMIC] Piano di importazione generato:")
    logger.info(f"   - Categorie rilevate: {len(analysis_result['categories'])}")
    logger.info(f"   - File totali processabili: {analysis_result['statistics']['total_files_analyzed']}")
    
    return analysis_result

if __name__ == "__main__":
    # Test della funzionalità
    import sys
    
    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    else:
        base_path = "./database/JSON"
    
    result = create_dynamic_import_plan(base_path)
    
    print("\n=== ANALISI DINAMICA COMPLETATA ===")
    for category, files in result['categories'].items():
        print(f"{category}: {len(files)} file") 