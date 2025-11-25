import pandas as pd
import re
import time
from datetime import datetime
from context_extraction import extract_context_window
from multiprocessing import Pool, cpu_count
import numpy as np


def _remove_overlapping_matches(matches):
    """
    Elimina matches que están completamente contenidos dentro de otros matches más largos.
    
    Args:
        matches: Lista de diccionarios con claves 'start', 'end', y otros datos del match
    
    Returns:
        Lista filtrada sin matches solapados (mantiene los más largos)
    """
    if not matches:
        return matches
    
    # Ordenar por posición de inicio, y por longitud descendente en caso de empate
    sorted_matches = sorted(matches, key=lambda x: (x['start'], -(x['end'] - x['start'])))
    
    filtered = []
    for current in sorted_matches:
        # Verificar si este match está completamente contenido en alguno ya aceptado
        is_contained = False
        for accepted in filtered:
            # Si current está completamente dentro de accepted, descartarlo
            if accepted['start'] <= current['start'] and current['end'] <= accepted['end']:
                # Solo descartar si no es el mismo match
                if not (accepted['start'] == current['start'] and accepted['end'] == current['end']):
                    is_contained = True
                    break
        
        if not is_contained:
            # También verificar si algún match ya aceptado está contenido en current
            # En ese caso, reemplazarlo con current (que es más largo)
            to_remove = []
            for i, accepted in enumerate(filtered):
                if current['start'] <= accepted['start'] and accepted['end'] <= current['end']:
                    if not (current['start'] == accepted['start'] and current['end'] == accepted['end']):
                        to_remove.append(i)
            
            # Eliminar los matches más cortos
            for i in reversed(to_remove):
                filtered.pop(i)
            
            filtered.append(current)
    
    return filtered


def _process_chunk(args):
    """
    Función auxiliar para procesar un chunk de datos en paralelo.
    Esta función se ejecuta en un proceso separado.
    
    Args:
        args: Tupla con (chunk_df, federal_laws_patterns, cdmx_laws_patterns, 
              official_cdmx_patterns, text_column, section_column, chunk_id)
    
    Returns:
        Lista de diccionarios con las entidades extraídas
    """
    chunk_df, federal_laws_patterns, cdmx_laws_patterns, official_cdmx_patterns, text_column, section_column, chunk_id = args
    
    results = []
    
    # Ordenar patrones por longitud (descendente) para procesar los más específicos primero
    federal_laws_sorted = sorted(federal_laws_patterns, key=lambda x: len(x[0]), reverse=True)
    cdmx_laws_sorted = sorted(cdmx_laws_patterns, key=lambda x: len(x[0]), reverse=True)
    official_cdmx_sorted = sorted(official_cdmx_patterns, key=lambda x: len(x[0]), reverse=True)
    
    # Agrupar patrones oficiales (ahora ordenados)
    official_patterns = [
        ('FEDERAL_LAWS', federal_laws_sorted),
        ('CDMX_LAWS', cdmx_laws_sorted), 
        ('CDMX_OFFICIAL', official_cdmx_sorted)
    ]
    
    for idx, row in chunk_df.iterrows():
        text = str(row[text_column]) if pd.notna(row[text_column]) else ""
        article_name = row[section_column] if pd.notna(row[section_column]) else ""
        
        if not text.strip():
            continue
        
        # Primero, recolectar TODOS los matches de este documento
        all_matches_for_doc = []
        
        # Extraer SOLO patrones OFICIALES
        for group_name, patterns in official_patterns:
            for pattern, entity_label in patterns:
                try:
                    matches = re.finditer(pattern, text, re.IGNORECASE)
                    for match in matches:
                        # Extraer 30 palabras antes y después de la coincidencia
                        context = extract_context_window(text, match.start(), match.end(), words_before=30, words_after=30)
                        
                        match_info = {
                            'doc_id': row.get('doc_id', ''),
                            'art_id': row.get('art_id', ''),
                            'document_name': row.get('document_name', ''),
                            'article_name': article_name,
                            'entity_text': match.group(0).strip(),
                            'entity_label': entity_label,
                            'pattern_group': group_name,
                            'before_context': context['before_context'],
                            'after_context': context['after_context'],
                            'full_context': context['full_context'],
                            'words_before_count': context['words_before_count'],
                            'words_after_count': context['words_after_count'],
                            'start': match.start(),
                            'end': match.end()
                        }
                        
                        all_matches_for_doc.append(match_info)
                        
                except re.error as e:
                    print(f"Error de regex en {group_name} (chunk {chunk_id}): {e}")
                    continue
        
        # Eliminar matches solapados (mantener los más largos/específicos)
        filtered_matches = _remove_overlapping_matches(all_matches_for_doc)
        
        # Agregar a resultados (sin las claves 'start' y 'end' que solo usamos para filtrar)
        for match in filtered_matches:
            match_cleaned = {k: v for k, v in match.items() if k not in ['start', 'end']}
            results.append(match_cleaned)
    
    return results


def extract_official_entities_parallel(df, federal_laws_regex, cdmx_laws_regex, gov_entity_regex, 
                                       text_column='text', section_column='article_name', n_jobs=None):
    """    
    Extrae SOLO menciones oficiales usando procesamiento PARALELO para mayor velocidad.
    
    Versión optimizada que divide el trabajo en múltiples procesos.
    
    Args:
        df: DataFrame con los documentos legales
        federal_laws_regex: Lista de patrones de leyes federales
        cdmx_laws_regex: Lista de patrones de leyes CDMX
        gov_entity_regex: Lista de patrones de entidades gubernamentales
        text_column: Nombre de la columna con el texto
        section_column: Nombre de la columna con el título de la sección
        n_jobs: Número de procesos paralelos (None = usar todos los CPUs disponibles)
    
    Returns:
        DataFrame con las entidades extraídas
    """
    start_time = time.time()
    
    print(f"Iniciando extracción de entidades OFICIALES (PARALELA) a las {datetime.now().strftime('%H:%M:%S')}")
    
    # Cargar SOLO patrones oficiales
    print("Cargando patrones oficiales...")
    federal_laws_patterns = [(pattern, category) for pattern, full_name, category in federal_laws_regex]
    cdmx_laws_patterns = [(pattern, category) for pattern, full_name, category in cdmx_laws_regex]
    official_cdmx_patterns = [(pattern.replace('\\\\\\\\', '\\\\'), category) for pattern, full_name, category in gov_entity_regex]
    
    # Determinar número de procesos
    if n_jobs is None:
        n_jobs = cpu_count()
    
    n_jobs = min(n_jobs, cpu_count())  # No exceder el número de CPUs disponibles
    
    print(f"Usando {n_jobs} procesos paralelos para {len(df)} documentos")
    
    # Dividir DataFrame en chunks
    chunk_size = max(1, len(df) // n_jobs)
    chunks = np.array_split(df, n_jobs)
    
    # Preparar argumentos para cada chunk
    chunk_args = [
        (chunk, federal_laws_patterns, cdmx_laws_patterns, official_cdmx_patterns, 
         text_column, section_column, i)
        for i, chunk in enumerate(chunks)
    ]
    
    # Procesar chunks en paralelo
    print(f"Procesando {len(chunks)} chunks en paralelo...")
    with Pool(processes=n_jobs) as pool:
        chunk_results = pool.map(_process_chunk, chunk_args)
    
    # Combinar todos los resultados
    all_results = []
    for chunk_result in chunk_results:
        all_results.extend(chunk_result)
    
    # Resumen final
    total_time = time.time() - start_time
    total_entities = len(all_results)
    
    print("=" * 70)
    print(f"EXTRACCIÓN COMPLETADA a las {datetime.now().strftime('%H:%M:%S')}")
    print(f"Tiempo total de procesamiento: {total_time:.2f} segundos ({total_time/60:.2f} minutos)")
    print(f"Documentos procesados: {len(df)}")
    print(f"Total de entidades oficiales extraídas: {total_entities}")
    print(f"Velocidad: {len(df)/total_time:.1f} documentos/segundo")
    print("=" * 70)
    
    return pd.DataFrame(all_results)


def extract_official_entities(df, federal_laws_regex, cdmx_laws_regex, gov_entity_regex, 
                              text_column='text', section_column='article_name'):
    """    
    Extrae SOLO menciones oficiales de:
    1. Leyes y reglamentos federales (patrones precisos)
    2. Leyes y reglamentos CDMX (patrones precisos)
    3. Entidades gubernamentales oficiales CDMX
    
    Args:
        df: DataFrame con los documentos legales
        federal_laws_regex: Lista de patrones de leyes federales
        cdmx_laws_regex: Lista de patrones de leyes CDMX
        gov_entity_regex: Lista de patrones de entidades gubernamentales
        text_column: Nombre de la columna con el texto
        section_column: Nombre de la columna con el título de la sección
    
    Returns:
        DataFrame con las entidades extraídas
    """
    start_time = time.time()
    results = []
    
    print(f"Iniciando extracción de entidades OFICIALES a las {datetime.now().strftime('%H:%M:%S')}")
    
    # Cargar SOLO patrones oficiales
    print("Cargando patrones oficiales...")
    federal_laws_patterns = [(pattern, category) for pattern, full_name, category in federal_laws_regex]
    cdmx_laws_patterns = [(pattern, category) for pattern, full_name, category in cdmx_laws_regex]
    official_cdmx_patterns = [(pattern.replace('\\\\\\\\', '\\\\'), category) for pattern, full_name, category in gov_entity_regex]
    
    # Ordenar patrones por longitud (descendente) para procesar los más específicos primero
    federal_laws_sorted = sorted(federal_laws_patterns, key=lambda x: len(x[0]), reverse=True)
    cdmx_laws_sorted = sorted(cdmx_laws_patterns, key=lambda x: len(x[0]), reverse=True)
    official_cdmx_sorted = sorted(official_cdmx_patterns, key=lambda x: len(x[0]), reverse=True)
    
    # Agrupar patrones oficiales (ahora ordenados)
    official_patterns = [
        ('FEDERAL_LAWS', federal_laws_sorted),
        ('CDMX_LAWS', cdmx_laws_sorted), 
        ('CDMX_OFFICIAL', official_cdmx_sorted)
    ]
    
    # Variables de seguimiento de progreso
    total_rows = len(df)
    total_matches = 0
    processed_rows = 0
    
    for idx, row in df.iterrows():
        processed_rows += 1
        
        # Indicador de progreso cada 100 filas
        if processed_rows % 100 == 0 or processed_rows == total_rows:
            elapsed = time.time() - start_time
            progress_pct = (processed_rows / total_rows) * 100
            print(f"Progreso: {processed_rows}/{total_rows} ({progress_pct:.1f}%) | "
                  f"Entidades encontradas: {total_matches} | Tiempo: {elapsed:.1f}s")
        
        text = str(row[text_column]) if pd.notna(row[text_column]) else ""
        article_name = row[section_column] if pd.notna(row[section_column]) else ""
        
        if not text.strip():
            continue
        
        # Primero, recolectar TODOS los matches de este documento
        all_matches_for_doc = []
        
        # Extraer SOLO patrones OFICIALES
        for group_name, patterns in official_patterns:
            for pattern, entity_label in patterns:
                try:
                    matches = re.finditer(pattern, text, re.IGNORECASE)
                    for match in matches:
                        # Extraer 30 palabras antes y después de la coincidencia
                        context = extract_context_window(text, match.start(), match.end(), words_before=30, words_after=30)
                        
                        match_info = {
                            'doc_id': row.get('doc_id', ''),
                            'art_id': row.get('art_id', ''),
                            'document_name': row.get('document_name', ''),
                            'article_name': article_name,
                            'entity_text': match.group(0).strip(),
                            'entity_label': entity_label,
                            'pattern_group': group_name,
                            'before_context': context['before_context'],
                            'after_context': context['after_context'],
                            'full_context': context['full_context'],
                            'words_before_count': context['words_before_count'],
                            'words_after_count': context['words_after_count'],
                            'start': match.start(),
                            'end': match.end()
                        }
                        
                        all_matches_for_doc.append(match_info)
                        
                except re.error as e:
                    print(f"Error de regex en {group_name}: {e}")
                    continue
        
        # Eliminar matches solapados (mantener los más largos/específicos)
        filtered_matches = _remove_overlapping_matches(all_matches_for_doc)
        
        # Agregar a resultados (sin las claves 'start' y 'end' que solo usamos para filtrar)
        for match in filtered_matches:
            match_cleaned = {k: v for k, v in match.items() if k not in ['start', 'end']}
            results.append(match_cleaned)
            total_matches += 1
    
    # Resumen final
    total_time = time.time() - start_time
    total_entities = len(results)
    
    print("=" * 70)
    print(f"EXTRACCIÓN COMPLETADA a las {datetime.now().strftime('%H:%M:%S')}")
    print(f"Tiempo total de procesamiento: {total_time:.2f} segundos")
    print(f"Documentos procesados: {processed_rows}")
    print(f"Total de entidades oficiales extraídas: {total_entities}")
    print("=" * 70)
    
    return pd.DataFrame(results)

