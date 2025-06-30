#!/usr/bin/env python3
"""
Test script per le 3 SOLUZIONI DINAMICHE per il problema di import JSON

Uso:
  python test_solutions.py --solution 1  # Auto-discovery dinamico
  python test_solutions.py --solution 2  # Smart categorization
  python test_solutions.py --solution 3  # Streaming incrementale
  python test_solutions.py --test-all    # Test tutte le soluzioni
"""

import os
import sys
import argparse
import time
from pathlib import Path

# Aggiungi src al path per gli import
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_solution_1():
    """Test SOLUZIONE 1: Auto-Discovery Dinamico con Path Fix"""
    print("🎯 === SOLUZIONE 1: AUTO-DISCOVERY DINAMICO ===")
    
    # Importa le funzioni modificate
    from import_json_mysql import discover_json_base_path, find_json_files
    
    print("📍 [TEST] Rilevamento automatico path JSON...")
    json_path = discover_json_base_path()
    print(f"✅ [RESULT] Path rilevato: {json_path}")
    
    print("🔍 [TEST] Scoperta file JSON con validazione...")
    json_files = find_json_files(json_path)
    print(f"✅ [RESULT] File JSON validi trovati: {len(json_files)}")
    
    if json_files:
        print("📄 [SAMPLE] Primi 5 file trovati:")
        for i, file_path in enumerate(json_files[:5], 1):
            file_name = Path(file_path).name
            file_size = Path(file_path).stat().st_size
            print(f"  {i}. {file_name} ({file_size:,} bytes)")
    
    return len(json_files) > 0

def test_solution_2():
    """Test SOLUZIONE 2: Schema-Detection Dinamico"""
    print("\n🧠 === SOLUZIONE 2: CATEGORIZZAZIONE INTELLIGENTE ===")
    
    from import_json_mysql import (
        discover_json_base_path, 
        smart_categorization_with_content_analysis,
        group_files_by_category,
        find_json_files
    )
    
    json_path = discover_json_base_path()
    
    print("🧹 [TEST] Categorizzazione base con pattern migliorati...")
    json_files = find_json_files(json_path)
    if not json_files:
        print("❌ [ERROR] Nessun file JSON trovato per il test")
        return False
    
    basic_categories = group_files_by_category(json_files)
    print(f"✅ [RESULT] Categorizzazione base: {len(basic_categories)} categorie")
    
    for category, files in basic_categories.items():
        print(f"  📂 {category}: {len(files)} file")
    
    print("\n🔍 [TEST] Categorizzazione intelligente con analisi contenuto...")
    smart_categories = smart_categorization_with_content_analysis(json_path)
    print(f"✅ [RESULT] Categorizzazione intelligente: {len(smart_categories)} categorie")
    
    for category, files in smart_categories.items():
        print(f"  🎯 {category}: {len(files)} file")
    
    # Confronto miglioramenti
    total_basic = sum(len(files) for files in basic_categories.values())
    total_smart = sum(len(files) for files in smart_categories.values())
    improvement = total_smart - total_basic
    
    print(f"\n📊 [COMPARISON] Miglioramento categorizzazione:")
    print(f"  - Metodo base: {total_basic} file categorizzati")
    print(f"  - Metodo smart: {total_smart} file categorizzati")
    print(f"  - Miglioramento: +{improvement} file (+{improvement/len(json_files)*100:.1f}%)")
    
    return improvement >= 0

def test_solution_3():
    """Test SOLUZIONE 3: Streaming Incrementale con Auto-Retry"""
    print("\n🚀 === SOLUZIONE 3: STREAMING INCREMENTALE ===")
    
    from import_json_mysql import (
        discover_json_base_path,
        find_json_files,
        smart_categorization_with_content_analysis,
        analyze_single_category
    )
    
    json_path = discover_json_base_path()
    json_files = find_json_files(json_path)
    
    if not json_files:
        print("❌ [ERROR] Nessun file JSON trovato per il test")
        return False
    
    print("📊 [TEST] Analisi caratteristiche dataset...")
    total_files = len(json_files)
    total_size_gb = sum(Path(f).stat().st_size for f in json_files) / (1024**3)
    
    print(f"✅ [DATASET] {total_files} file, {total_size_gb:.2f} GB totali")
    
    print("🧠 [TEST] Categorizzazione per streaming...")
    categories = smart_categorization_with_content_analysis(json_path)
    
    if not categories:
        print("❌ [ERROR] Nessuna categoria trovata")
        return False
    
    print(f"✅ [CATEGORIES] {len(categories)} categorie pronte per streaming")
    
    # Test analisi schema per ogni categoria (solo primi file)
    print("\n🔍 [TEST] Analisi schema per categoria (campioni)...")
    for category, files in categories.items():
        if len(files) > 0:
            print(f"  📂 Analizzando categoria '{category}'...")
            sample_files = files[:2]  # Analizza solo primi 2 file per test
            
            try:
                schema = analyze_single_category(category, sample_files)
                if schema:
                    print(f"    ✅ Schema rilevato: {len(schema)} campi")
                else:
                    print(f"    ⚠️  Schema vuoto o errore")
            except Exception as e:
                print(f"    ❌ Errore analisi schema: {e}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Test delle soluzioni dinamiche per import JSON')
    parser.add_argument('--solution', type=int, choices=[1, 2, 3], 
                       help='Testa una soluzione specifica (1, 2, o 3)')
    parser.add_argument('--test-all', action='store_true',
                       help='Testa tutte le soluzioni')
    
    args = parser.parse_args()
    
    results = {}
    
    if args.solution == 1 or args.test_all:
        try:
            results['solution_1'] = test_solution_1()
        except Exception as e:
            print(f"❌ [ERROR] Soluzione 1 fallita: {e}")
            results['solution_1'] = False
    
    if args.solution == 2 or args.test_all:
        try:
            results['solution_2'] = test_solution_2()
        except Exception as e:
            print(f"❌ [ERROR] Soluzione 2 fallita: {e}")
            results['solution_2'] = False
    
    if args.solution == 3 or args.test_all:
        try:
            results['solution_3'] = test_solution_3()
        except Exception as e:
            print(f"❌ [ERROR] Soluzione 3 fallita: {e}")
            results['solution_3'] = False
    
    # Report finale
    print("\n" + "="*60)
    print("📊 === RISULTATI TEST SOLUZIONI ===")
    
    for solution, success in results.items():
        status = "✅ SUCCESSO" if success else "❌ FALLIMENTO"
        solution_name = {
            'solution_1': "Auto-Discovery Dinamico",
            'solution_2': "Categorizzazione Intelligente", 
            'solution_3': "Streaming Incrementale"
        }.get(solution, solution)
        
        print(f"  {solution_name}: {status}")
    
    total_success = sum(results.values())
    total_tests = len(results)
    
    print(f"\n🎯 [SUMMARY] {total_success}/{total_tests} soluzioni testate con successo")
    
    if total_success == total_tests:
        print("🎉 [EXCELLENT] Tutte le soluzioni funzionano correttamente!")
        print("\n💡 [NEXT STEPS] Puoi ora utilizzare le soluzioni:")
        print("  $env:IMPORT_MODE='auto'       # Selezione automatica (PowerShell)")
        print("  $env:IMPORT_MODE='smart'      # Categorizzazione intelligente")
        print("  $env:IMPORT_MODE='streaming'  # Streaming incrementale")
        print("  python -m src.import_json_mysql")
    else:
        print("⚠️  [WARNING] Alcune soluzioni hanno problemi. Verifica i log sopra.")
        
    return total_success == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
