import pandas as pd
import json
import time
import re
from openai import OpenAI
from multiprocessing import Pool, cpu_count
import numpy as np

def filter_official_regex_matches(df, entity_text_col='entity_text'):
    """
    Filtra las filas del DataFrame que ya tienen match con los regex oficiales
    de CDMX y leyes federales. Devuelve solo las filas SIN match que necesitan
    procesamiento con LLM.
    
    IMPORTANTE: Esta función excluye menciones donde la ley es parte de otro documento,
    como "Reglamento de la Ley X" o "Código de la Ley Y", para evitar falsos positivos.
    Solo hace match cuando la ley es la entidad principal del texto.
    
    Ejemplo:
        - "Ley de Salud de la Ciudad de México" → MATCH ✓
        - "Reglamento de la Ley de Salud de la Ciudad de México" → NO MATCH ✗
        - "Código de la Ley de Transparencia" → NO MATCH ✗
    
    Args:
        df (pd.DataFrame): DataFrame con menciones de leyes
        entity_text_col (str): Nombre de la columna con el texto de la entidad
    
    Returns:
        tuple: (df_sin_match, df_con_match, estadisticas)
            - df_sin_match: Menciones que necesitan procesamiento con LLM
            - df_con_match: Menciones ya identificadas con regex
            - estadisticas: Dict con conteos y porcentajes
    """
    print("=== FILTRANDO MENCIONES CON REGEX OFICIALES ===")
    print(f"Total de menciones a analizar: {len(df)}")
    
    # Importar los patrones de regex
    import sys
    import os
    
    # Agregar path de regex
    regex_path = os.path.join(os.path.dirname(__file__), '../regex')
    sys.path.append(regex_path)
    
    from cdmx_laws_patterns_precise import LAWS_REGEX as CDMX_PATTERNS
    from federal_laws_patterns_precise import LAWS_REGEX as FEDERAL_PATTERNS
    
    # Combinar todos los patrones
    all_patterns = CDMX_PATTERNS + FEDERAL_PATTERNS
    
    print(f"Total de patrones regex a aplicar: {len(all_patterns)}")
    
    # Marcar las filas que tienen match
    df_copy = df.copy()
    df_copy['has_regex_match'] = False
    df_copy['matched_pattern'] = None
    df_copy['matched_law'] = None
    
    matched_count = 0
    
    # Patrones a excluir (cuando la ley es parte de otro documento)
    exclude_patterns = [
        r'\bReglamento\s+de\s+la\s+',
        r'\bReglamento\s+',
        r'\bCódigo\s+',
        r'\bCodigo\s+',
        r'\bNorma\s+',
        r'\bDecreto\s+',
    ]
    
    for idx, row in df_copy.iterrows():
        entity_text = str(row[entity_text_col])
        
        # Probar cada patrón de ley oficial
        for pattern, law_name, category in all_patterns:
            match = re.search(pattern, entity_text, re.IGNORECASE)
            if match:
                # PASO 1: Encontramos el patrón de la ley en el texto
                # Ejemplo: "Reglamento de la Ley de Salud" → encuentra "Ley de Salud"
                
                # PASO 2: Verificar que no esté precedido por palabras que indican otro tipo de documento
                # Queremos EXCLUIR casos como:
                #   - "Reglamento de la Ley X" (es un reglamento, no la ley)
                #   - "Código de la Ley Y" (es un código, no la ley)
                is_valid_match = True
                start_pos = match.start()
                
                # Obtener texto antes del match
                # Ejemplo: "Reglamento de la Ley de Salud" → text_before = "Reglamento de la "
                text_before = entity_text[:start_pos]
                
                # PASO 3: Verificar si hay un patrón de exclusión justo antes del match
                # El $ al final del patrón asegura que termine justo antes del match
                for exclude_pattern in exclude_patterns:
                    if re.search(exclude_pattern + r'$', text_before, re.IGNORECASE):
                        # Encontramos "Reglamento ", "Código ", etc. antes de la ley
                        # Este NO es un match válido
                        is_valid_match = False
                        break
                
                if is_valid_match:
                    # Este es un match válido: la ley es la entidad principal
                    df_copy.at[idx, 'has_regex_match'] = True
                    df_copy.at[idx, 'matched_pattern'] = pattern
                    df_copy.at[idx, 'matched_law'] = law_name
                    matched_count += 1
                    break  # Ya encontró match, no seguir buscando
        
        # Mostrar progreso cada 100 registros
        if (idx + 1) % 100 == 0:
            print(f"  Procesados: {idx + 1}/{len(df_copy)}")
    
    # Separar en dos DataFrames
    df_sin_match = df_copy[df_copy['has_regex_match'] == False].copy()
    df_con_match = df_copy[df_copy['has_regex_match'] == True].copy()
    
    # Eliminar columnas auxiliares del DataFrame sin match (las que irán a LLM)
    df_sin_match = df_sin_match.drop(['has_regex_match', 'matched_pattern', 'matched_law'], axis=1)
    
    # Estadísticas
    stats = {
        'total': len(df),
        'con_match_regex': len(df_con_match),
        'sin_match_regex': len(df_sin_match),
        'porcentaje_filtrado': (len(df_con_match) / len(df) * 100) if len(df) > 0 else 0
    }
    
    print(f"\n=== RESULTADOS DEL FILTRADO ===")
    print(f"Total de menciones: {stats['total']}")
    print(f"Con match en regex (filtradas): {stats['con_match_regex']}")
    print(f"Sin match en regex (para LLM): {stats['sin_match_regex']}")
    print(f"Porcentaje filtrado: {stats['porcentaje_filtrado']:.1f}%")
    
    # Mostrar ejemplos de menciones filtradas
    if len(df_con_match) > 0:
        print(f"\n=== EJEMPLOS DE MENCIONES FILTRADAS (YA TIENEN MATCH) ===")
        for i, row in df_con_match.head(10).iterrows():
            print(f"{i+1}. {row[entity_text_col][:80]}")
            print(f"   Match: {row['matched_law']}")
    
    # Mostrar ejemplos de menciones sin match
    if len(df_sin_match) > 0:
        print(f"\n=== EJEMPLOS DE MENCIONES SIN MATCH (PARA LLM) ===")
        for i, row in df_sin_match.head(10).iterrows():
            print(f"{i+1}. {row[entity_text_col][:80]}")
    
    return df_sin_match, df_con_match, stats

def create_prompt_match_laws(entity_text, cdmx_laws_list):
    """
    Crea un prompt para que OpenAI haga matching entre un entity_text de ley
    y la lista oficial de leyes de CDMX, devolviendo directamente el doc_id.
    """
    prompt = f"""
Eres un experto en leyes de la Ciudad de México. Tu tarea es encontrar el match más preciso entre un texto que menciona una ley y la lista oficial de leyes de CDMX.

TEXTO A MATCHEAR:
"{entity_text}"

LISTA OFICIAL DE LEYES CDMX:
{cdmx_laws_list}

INSTRUCCIONES:
1. Analiza el texto y encuentra la ley oficial que mejor coincida
2. Considera variaciones en nombres, abreviaciones, y diferencias menores
3. Si encuentras un match claro, responde con el doc_id exacto
4. Si no hay match claro, responde "NO_MATCH"
5. Si hay ambigüedad entre varios matches, elige el más probable

FORMATO DE RESPUESTA:
- Si hay match seguro: "MATCH: [doc_id]"
- Si hay match ambiguo: "AMBIGUOUS: [doc_id]"
- Si no hay match: "NO_MATCH"

IMPORTANTE: 
- Usa EXACTAMENTE el doc_id que aparece en la lista, sin modificaciones
- El doc_id es la cadena de 8 caracteres alfanuméricos (ej: 234F69A3)
- No incluyas el nombre de la ley, solo el doc_id

RESPUESTA:
"""
    return prompt

def validate_doc_id(doc_id, cdmx_laws_df):
    """
    Valida que el doc_id existe en el DataFrame y devuelve el nombre correspondiente.
    
    Args:
        doc_id (str): ID del documento a validar
        cdmx_laws_df (pd.DataFrame): DataFrame con leyes oficiales
    
    Returns:
        tuple: (is_valid, nombre_ley)
    """
    if not doc_id or doc_id == '':
        return False, ''
    
    matches = cdmx_laws_df[cdmx_laws_df['doc_id'] == doc_id]
    if not matches.empty:
        return True, matches.iloc[0]['nombre']
    
    return False, ''

def match_law_with_openai(entity_text, art_id, cdmx_laws_df, client, delay_seconds=1, temperature=0.2):
    """
    Hace matching de un entity_text de ley con la lista oficial usando OpenAI.
    Devuelve directamente el doc_id.
    
    Args:
        entity_text (str): Texto de la entidad a matchear
        art_id: ID de la fila en el DataFrame original
        cdmx_laws_df (pd.DataFrame): DataFrame con leyes oficiales
        client: Cliente de OpenAI
        delay_seconds (float): Tiempo de espera entre llamadas en segundos
        temperature (float): Temperatura del modelo (0.0-1.0)
    
    Returns:
        dict: Resultado del matching
    """
    # Crear lista de leyes oficiales con doc_id
    cdmx_list = []
    for idx, row in cdmx_laws_df.iterrows():
        cdmx_list.append(f"- {row['nombre']} (ID: {row['doc_id']})")
    
    cdmx_laws_text = "\n".join(cdmx_list)
    
    # Crear prompt
    prompt = create_prompt_match_laws(entity_text, cdmx_laws_text)
    
    try:
        # Llamar a OpenAI
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature
        )
        
        result = response.choices[0].message.content.strip()
        
        # Parsear resultado
        if result.startswith("MATCH:"):
            doc_id = result.replace("MATCH:", "").strip()
            # Validar que el doc_id existe
            is_valid, nombre_ley = validate_doc_id(doc_id, cdmx_laws_df)
            
            if is_valid:
                return {
                    'art_id': art_id,
                    'entity_text': entity_text,
                    'cdmx_official_name': nombre_ley,
                    'cdmx_doc_id': doc_id,
                    'match_quality': 'safe',
                    'openai_response': result
                }
            else:
                print(f"Doc_id inválido devuelto por LLM: '{doc_id}'")
                return {
                    'art_id': art_id,
                    'entity_text': entity_text,
                    'cdmx_official_name': '',
                    'cdmx_doc_id': '',
                    'match_quality': 'error',
                    'openai_response': f"Doc_id inválido: {doc_id}"
                }
                
        elif result.startswith("AMBIGUOUS:"):
            doc_id = result.replace("AMBIGUOUS:", "").strip()
            # Validar que el doc_id existe
            is_valid, nombre_ley = validate_doc_id(doc_id, cdmx_laws_df)
            
            if is_valid:
                return {
                    'art_id': art_id,
                    'entity_text': entity_text,
                    'cdmx_official_name': nombre_ley,
                    'cdmx_doc_id': doc_id,
                    'match_quality': 'ambiguous',
                    'openai_response': result
                }
            else:
                print(f"Doc_id inválido devuelto por LLM: '{doc_id}'")
                return {
                    'art_id': art_id,
                    'entity_text': entity_text,
                    'cdmx_official_name': '',
                    'cdmx_doc_id': '',
                    'match_quality': 'error',
                    'openai_response': f"Doc_id inválido: {doc_id}"
                }
        else:
            return {
                'art_id': art_id,
                'entity_text': entity_text,
                'cdmx_official_name': '',
                'cdmx_doc_id': '',
                'match_quality': 'no_match',
                'openai_response': result
            }
            
    except Exception as e:
        return {
            'art_id': art_id,
            'entity_text': entity_text,
            'cdmx_official_name': '',
            'cdmx_doc_id': '',
            'match_quality': 'error',
            'openai_response': f"Error: {str(e)}"
        }
    finally:
        # Esperar el tiempo especificado antes de continuar
        if delay_seconds > 0:
            time.sleep(delay_seconds)

def apply_openai_law_matching(mentions_df, cdmx_laws_df, client, 
                               entity_text_col='entity_text',
                               art_id_col='art_id',
                               cdmx_name_col='nombre',
                               batch_size=10,
                               delay_seconds=1,
                               temperature=0.2):
    """
    Aplica matching de leyes usando OpenAI a un DataFrame de menciones.
    LLM devuelve directamente doc_id.
    Procesa TODAS las filas basándose en art_id y entity_text.
    
    Args:
        mentions_df (pd.DataFrame): DataFrame con menciones de leyes
        cdmx_laws_df (pd.DataFrame): DataFrame con leyes oficiales CDMX
        client: Cliente de OpenAI
        entity_text_col (str): Nombre de la columna con entity_text
        art_id_col (str): Nombre de la columna con art_id
        cdmx_name_col (str): Nombre de la columna con nombres oficiales
        batch_size (int): Tamaño del lote para procesar
        delay_seconds (float): Tiempo de espera entre llamadas en segundos
        temperature (float): Temperatura del modelo (0.0-1.0)
    
    Returns:
        pd.DataFrame: DataFrame con los matches encontrados
    """
    print("=== MATCHING DE LEYES CON OPENAI ===")
    print(f"Menciones a procesar: {len(mentions_df)}")
    print(f"Leyes oficiales CDMX: {len(cdmx_laws_df)}")
    print(f"Tiempo de espera entre llamadas: {delay_seconds} segundos")
    print(f"Temperatura del modelo: {temperature}")
    print()
    
    # Filtrar filas con valores válidos en entity_text_col
    valid_rows = mentions_df[mentions_df[entity_text_col].notna() & 
                           (mentions_df[entity_text_col].astype(str).str.strip() != '')]
    
    print(f"Filas válidas para procesar: {len(valid_rows)} (de {len(mentions_df)} total)")
    
    if len(valid_rows) == 0:
        print("No hay filas válidas para procesar.")
        return pd.DataFrame()
    
    # Calcular tiempo estimado total
    estimated_time_minutes = (len(valid_rows) * delay_seconds) / 60
    print(f"Tiempo estimado total: {estimated_time_minutes:.1f} minutos")
    print()
    
    # Procesar TODAS las filas válidas (no solo únicas)
    print(f"Procesando todas las filas válidas basándose en art_id y entity_text...")
    
    results = []
    start_time = time.time()
    invalid_doc_ids = []
    
    # Procesar en lotes
    for i in range(0, len(valid_rows), batch_size):
        batch = valid_rows.iloc[i:i+batch_size]
        print(f"Procesando lote {i//batch_size + 1}/{(len(valid_rows) + batch_size - 1)//batch_size}")
        
        for idx, row in batch.iterrows():
            entity_text = str(row[entity_text_col]).strip()  # Convertir a string y limpiar
            art_id = row[art_id_col]
            
            # Verificar que entity_text no esté vacío
            if not entity_text or entity_text == 'nan':
                print(f"  Saltando art_id {art_id}: entity_text vacío o NaN")
                continue
                
            print(f"  Procesando art_id {art_id}: {entity_text[:50]}...")
            result = match_law_with_openai(entity_text, art_id, cdmx_laws_df, client, delay_seconds, temperature)
            results.append(result)
            
            # Recopilar doc_ids inválidos para análisis
            if result['match_quality'] == 'error' and 'Doc_id inválido' in result['openai_response']:
                invalid_doc_ids.append(result['openai_response'])
            
            # Mostrar progreso cada 10 registros
            if len(results) % 10 == 0:
                elapsed_time = time.time() - start_time
                remaining_items = len(valid_rows) - len(results)
                estimated_remaining_time = (remaining_items * delay_seconds) / 60
                print(f"    Progreso: {len(results)}/{len(valid_rows)} registros procesados")
                print(f"    Tiempo transcurrido: {elapsed_time/60:.1f} minutos")
                print(f"    Tiempo restante estimado: {estimated_remaining_time:.1f} minutos")
                print()
    
    # Convertir a DataFrame
    results_df = pd.DataFrame(results)
    
    # Estadísticas
    total = len(results_df)
    safe_matches = len(results_df[results_df['match_quality'] == 'safe'])
    ambiguous_matches = len(results_df[results_df['match_quality'] == 'ambiguous'])
    no_matches = len(results_df[results_df['match_quality'] == 'no_match'])
    errors = len(results_df[results_df['match_quality'] == 'error'])
    
    # Estadísticas de doc_id
    with_doc_id = len(results_df[results_df['cdmx_doc_id'] != ''])
    without_doc_id = len(results_df[results_df['cdmx_doc_id'] == ''])
    invalid_ids = len(invalid_doc_ids)
    
    total_time = time.time() - start_time
    
    print(f"\n=== ESTADÍSTICAS DE MATCHING ===")
    print(f"Total procesadas: {total}")
    print(f"Matches seguros: {safe_matches}")
    print(f"Matches ambiguos: {ambiguous_matches}")
    print(f"Sin matches: {no_matches}")
    print(f"Errores: {errors}")
    print(f"Tasa de éxito: {((safe_matches + ambiguous_matches) / total * 100):.1f}%")
    print(f"Con doc_id válido: {with_doc_id}")
    print(f"Sin doc_id: {without_doc_id}")
    print(f"Doc_ids inválidos: {invalid_ids}")
    print(f"Tasa de doc_id válido: {(with_doc_id / total * 100):.1f}%")
    print(f"Tiempo total transcurrido: {total_time/60:.1f} minutos")
    
    # Mostrar ejemplos de doc_ids inválidos si los hay
    if invalid_ids > 0:
        print(f"\n=== DOC_IDS INVÁLIDOS ENCONTRADOS ===")
        for invalid_id in invalid_doc_ids[:5]:  # Mostrar solo los primeros 5
            print(f"  - {invalid_id}")
        if len(invalid_doc_ids) > 5:
            print(f"  ... y {len(invalid_doc_ids) - 5} más")
    
    return results_df


# ============================================================================
# VERSIONES OPTIMIZADAS CON PARALELIZACIÓN Y DEDUPLICACIÓN
# ============================================================================

def _process_regex_chunk(args):
    """
    Función auxiliar para procesar un chunk de menciones con regex en paralelo.
    Similar al patrón usado en entity_extraction_functions.py
    
    Args:
        args: Tupla con (chunk_df, all_patterns, entity_text_col, exclude_patterns, chunk_id)
    
    Returns:
        Lista de diccionarios con los resultados del matching
    """
    chunk_df, all_patterns, entity_text_col, exclude_patterns, chunk_id = args
    
    results = []
    
    for idx, row in chunk_df.iterrows():
        entity_text = str(row[entity_text_col])
        
        has_match = False
        matched_pattern = None
        matched_law = None
        
        # Probar cada patrón de ley oficial
        for pattern, law_name, category in all_patterns:
            match = re.search(pattern, entity_text, re.IGNORECASE)
            if match:
                is_valid_match = True
                start_pos = match.start()
                text_before = entity_text[:start_pos]
                
                # Verificar si hay un patrón de exclusión justo antes del match
                for exclude_pattern in exclude_patterns:
                    if re.search(exclude_pattern + r'$', text_before, re.IGNORECASE):
                        is_valid_match = False
                        break
                
                if is_valid_match:
                    has_match = True
                    matched_pattern = pattern
                    matched_law = law_name
                    break  # Ya encontró match, no seguir buscando
        
        results.append({
            'index': idx,
            'has_match': has_match,
            'matched_pattern': matched_pattern,
            'matched_law': matched_law
        })
    
    return results


def filter_official_regex_matches_parallel(df, entity_text_col='entity_text', n_jobs=None):
    """
    Versión PARALELA del filtrado de regex. Mucho más rápida para grandes volúmenes.
    Basada en el patrón de extract_official_entities_parallel de entity_extraction_functions.py
    
    Filtra las filas del DataFrame que ya tienen match con los regex oficiales
    de CDMX y leyes federales usando procesamiento en paralelo.
    
    Args:
        df (pd.DataFrame): DataFrame con menciones de leyes
        entity_text_col (str): Nombre de la columna con el texto de la entidad
        n_jobs (int): Número de procesos paralelos (None = usar todos los CPUs)
    
    Returns:
        tuple: (df_sin_match, df_con_match, estadisticas)
    """
    print("=== FILTRANDO MENCIONES CON REGEX OFICIALES (PARALELO) ===")
    print(f"Total de menciones a analizar: {len(df)}")
    
    # Importar los patrones de regex
    import sys
    import os
    
    regex_path = os.path.join(os.path.dirname(__file__), '../regex')
    sys.path.append(regex_path)
    
    from cdmx_laws_patterns_precise import LAWS_REGEX as CDMX_PATTERNS
    from federal_laws_patterns_precise import LAWS_REGEX as FEDERAL_PATTERNS
    
    # Combinar todos los patrones
    all_patterns = CDMX_PATTERNS + FEDERAL_PATTERNS
    
    print(f"Total de patrones regex a aplicar: {len(all_patterns)}")
    
    # Patrones a excluir
    exclude_patterns = [
        r'\bReglamento\s+de\s+la\s+',
        r'\bReglamento\s+',
        r'\bCódigo\s+',
        r'\bCodigo\s+',
        r'\bNorma\s+',
        r'\bDecreto\s+',
    ]
    
    # Determinar número de procesos
    if n_jobs is None:
        n_jobs = cpu_count()
    n_jobs = min(n_jobs, cpu_count())
    
    print(f"Usando {n_jobs} procesos paralelos")
    
    # Dividir DataFrame en chunks
    chunk_size = max(1, len(df) // n_jobs)
    chunks = np.array_split(df, n_jobs)
    
    # Preparar argumentos para cada chunk
    chunk_args = [
        (chunk, all_patterns, entity_text_col, exclude_patterns, i)
        for i, chunk in enumerate(chunks)
    ]
    
    # Procesar chunks en paralelo
    print(f"Procesando {len(chunks)} chunks en paralelo...")
    with Pool(processes=n_jobs) as pool:
        chunk_results = pool.map(_process_regex_chunk, chunk_args)
    
    # Combinar todos los resultados
    all_results = []
    for chunk_result in chunk_results:
        all_results.extend(chunk_result)
    
    # Crear DataFrame con resultados
    df_copy = df.copy()
    df_copy['has_regex_match'] = False
    df_copy['matched_pattern'] = None
    df_copy['matched_law'] = None
    
    for result in all_results:
        idx = result['index']
        df_copy.at[idx, 'has_regex_match'] = result['has_match']
        df_copy.at[idx, 'matched_pattern'] = result['matched_pattern']
        df_copy.at[idx, 'matched_law'] = result['matched_law']
    
    # Separar en dos DataFrames
    df_sin_match = df_copy[df_copy['has_regex_match'] == False].copy()
    df_con_match = df_copy[df_copy['has_regex_match'] == True].copy()
    
    # Eliminar columnas auxiliares del DataFrame sin match
    df_sin_match = df_sin_match.drop(['has_regex_match', 'matched_pattern', 'matched_law'], axis=1)
    
    # Estadísticas
    stats = {
        'total': len(df),
        'con_match_regex': len(df_con_match),
        'sin_match_regex': len(df_sin_match),
        'porcentaje_filtrado': (len(df_con_match) / len(df) * 100) if len(df) > 0 else 0
    }
    
    print(f"\n=== RESULTADOS DEL FILTRADO ===")
    print(f"Total de menciones: {stats['total']}")
    print(f"Con match en regex (filtradas): {stats['con_match_regex']}")
    print(f"Sin match en regex (para LLM): {stats['sin_match_regex']}")
    print(f"Porcentaje filtrado: {stats['porcentaje_filtrado']:.1f}%")
    
    # Mostrar ejemplos
    if len(df_con_match) > 0:
        print(f"\n=== EJEMPLOS DE MENCIONES FILTRADAS (YA TIENEN MATCH) ===")
        for i, (idx, row) in enumerate(df_con_match.head(5).iterrows()):
            print(f"{i+1}. {row[entity_text_col][:80]}")
            print(f"   Match: {row['matched_law']}")
    
    if len(df_sin_match) > 0:
        print(f"\n=== EJEMPLOS DE MENCIONES SIN MATCH (PARA LLM) ===")
        for i, (idx, row) in enumerate(df_sin_match.head(5).iterrows()):
            print(f"{i+1}. {row[entity_text_col][:80]}")
    
    return df_sin_match, df_con_match, stats


def apply_openai_law_matching_deduplicated(mentions_df, cdmx_laws_df, client, 
                                            entity_text_col='entity_text',
                                            art_id_col='art_id',
                                            delay_seconds=1,
                                            temperature=0.2):
    """
    Versión OPTIMIZADA que procesa solo menciones ÚNICAS y luego mapea
    los resultados a todas las filas originales.
    
    
    Args:
        mentions_df (pd.DataFrame): DataFrame con menciones de leyes
        cdmx_laws_df (pd.DataFrame): DataFrame con leyes oficiales CDMX
        client: Cliente de OpenAI
        entity_text_col (str): Nombre de la columna con entity_text
        art_id_col (str): Nombre de la columna con art_id
        delay_seconds (float): Tiempo de espera entre llamadas (default 0.5s)
        temperature (float): Temperatura del modelo (0.0-1.0)
    
    Returns:
        pd.DataFrame: DataFrame con los matches encontrados para TODAS las filas
    """
    print("=== MATCHING DE LEYES CON OPENAI (OPTIMIZADO - DEDUPLICADO) ===")
    print(f"Total de menciones (con duplicados): {len(mentions_df)}")
    
    # Filtrar filas válidas
    valid_rows = mentions_df[mentions_df[entity_text_col].notna() & 
                           (mentions_df[entity_text_col].astype(str).str.strip() != '')]
    
    print(f"Filas válidas: {len(valid_rows)} (de {len(mentions_df)} total)")
    
    if len(valid_rows) == 0:
        print("No hay filas válidas para procesar.")
        return pd.DataFrame()
    
    # 1. Obtener menciones ÚNICAS
    unique_entity_texts = valid_rows[entity_text_col].astype(str).str.strip().unique()
    
    print(f"\nMenciones ÚNICAS a procesar: {len(unique_entity_texts)}")
    print(f" Reducción: {len(valid_rows) - len(unique_entity_texts)} llamadas ahorradas")
    print(f"Ahorro de tiempo estimado: {(len(valid_rows) - len(unique_entity_texts)) * delay_seconds / 60:.1f} minutos")
    
    # Calcular tiempo estimado
    estimated_time_minutes = (len(unique_entity_texts) * delay_seconds) / 60
    print(f"Tiempo estimado total: {estimated_time_minutes:.1f} minutos")
    print(f"Leyes oficiales CDMX en catálogo: {len(cdmx_laws_df)}")
    print(f"Temperatura del modelo: {temperature}")
    print()
    
    # 2. Procesar solo menciones ÚNICAS
    cache = {}  # Cachear resultados por entity_text
    start_time = time.time()
    
    print(f"Procesando menciones únicas...")
    for i, entity_text in enumerate(unique_entity_texts, 1):
        # Llamar a OpenAI
        result = match_law_with_openai(
            entity_text=entity_text,
            art_id='temp',  # Temporal, será reemplazado después
            cdmx_laws_df=cdmx_laws_df,
            client=client,
            delay_seconds=delay_seconds,
            temperature=temperature
        )
        
        # Guardar en caché (solo los datos necesarios)
        cache[entity_text] = {
            'cdmx_official_name': result['cdmx_official_name'],
            'cdmx_doc_id': result['cdmx_doc_id'],
            'match_quality': result['match_quality'],
            'openai_response': result['openai_response']
        }
        
        # Mostrar progreso cada 10 menciones únicas
        if i % 10 == 0:
            elapsed_time = time.time() - start_time
            remaining_items = len(unique_entity_texts) - i
            estimated_remaining_time = (remaining_items * delay_seconds) / 60
            print(f"  Progreso: {i}/{len(unique_entity_texts)} menciones únicas procesadas")
            print(f"  Tiempo transcurrido: {elapsed_time/60:.1f} minutos")
            print(f"  Tiempo restante estimado: {estimated_remaining_time:.1f} minutos")
            print()
    
    print(f"\n✓ Procesamiento de menciones únicas completado")
    print(f"  Total procesado: {len(cache)} menciones únicas")
    
    # 3. Mapear resultados del caché a TODAS las filas originales
    print(f"\nMapeando resultados a todas las {len(valid_rows)} filas originales...")
    results = []
    
    for idx, row in valid_rows.iterrows():
        entity_text = str(row[entity_text_col]).strip()
        
        if entity_text in cache:
            cached_result = cache[entity_text]
            results.append({
                'art_id': row[art_id_col],
                'entity_text': entity_text,
                'cdmx_official_name': cached_result['cdmx_official_name'],
                'cdmx_doc_id': cached_result['cdmx_doc_id'],
                'match_quality': cached_result['match_quality'],
                'openai_response': cached_result['openai_response']
            })
        else:
            # Caso raro: no debería pasar, pero por seguridad
            print(f"Warning: entity_text no encontrado en caché: {entity_text[:50]}")
            results.append({
                'art_id': row[art_id_col],
                'entity_text': entity_text,
                'cdmx_official_name': '',
                'cdmx_doc_id': '',
                'match_quality': 'error',
                'openai_response': 'No encontrado en caché'
            })
    
    results_df = pd.DataFrame(results)
    
    # Estadísticas finales
    total = len(results_df)
    safe_matches = len(results_df[results_df['match_quality'] == 'safe'])
    ambiguous_matches = len(results_df[results_df['match_quality'] == 'ambiguous'])
    no_matches = len(results_df[results_df['match_quality'] == 'no_match'])
    errors = len(results_df[results_df['match_quality'] == 'error'])
    
    with_doc_id = len(results_df[results_df['cdmx_doc_id'] != ''])
    without_doc_id = len(results_df[results_df['cdmx_doc_id'] == ''])
    
    total_time = time.time() - start_time
    
    print(f"\n=== ESTADÍSTICAS FINALES ===")
    print(f"Total de filas con resultados: {total}")
    print(f"Menciones únicas procesadas: {len(cache)}")
    print(f"Matches seguros: {safe_matches} ({safe_matches/total*100:.1f}%)")
    print(f"Matches ambiguos: {ambiguous_matches} ({ambiguous_matches/total*100:.1f}%)")
    print(f"Sin matches: {no_matches} ({no_matches/total*100:.1f}%)")
    print(f"Errores: {errors} ({errors/total*100:.1f}%)")
    print(f"\nTasa de éxito general: {((safe_matches + ambiguous_matches) / total * 100):.1f}%")
    print(f"Con doc_id válido: {with_doc_id} ({with_doc_id/total*100:.1f}%)")
    print(f"Sin doc_id: {without_doc_id} ({without_doc_id/total*100:.1f}%)")
    print(f"\nTiempo total: {total_time/60:.1f} minutos")
    print(f"⚡ Velocidad: {len(cache)/(total_time/60):.1f} menciones únicas/minuto")
    
    return results_df

