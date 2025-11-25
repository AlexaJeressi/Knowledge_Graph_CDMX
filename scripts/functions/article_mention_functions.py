import pandas as pd
import re
import time
from datetime import datetime
from context_extraction import extract_context_window


def extract_article_mentions(df, text_column='text', section_column='article_name'):
    """    
    Extrae menciones de artículos en documentos legales:
    - Artículos individuales (artículo 50)
    - Múltiples artículos con conectores (artículos 50 y 325)
    - Rangos (artículos del 10 al 15)
    - Referencias relativas (artículo anterior, siguiente, etc.)
    
    Args:
        df: DataFrame con los documentos legales
        text_column: Nombre de la columna con el texto
        section_column: Nombre de la columna con el título de la sección
    
    Returns:
        DataFrame con las menciones de artículos extraídas
    """
    start_time = time.time()
    results = []
    
    print(f"Iniciando extracción de MENCIONES DE ARTÍCULOS a las {datetime.now().strftime('%H:%M:%S')}")
    
    # Patrones para detectar menciones de artículos
    # Incluye sufijos latinos: bis, ter, quáter, quartus, quintus, sextus, septimus, octavus, novenus, decimus
    # y ordinales en español: quinto, sexto, séptimo, octavo, noveno, décimo
    # También incluye: letras (25 a, 25 b), números adicionales (bis 3, ter 4), números con guión (449-29, 63-Bis)
    
    # Definir el patrón de sufijos completo (para reutilización)
    # Incluye opción de número adicional (bis 3, ter 4) O letra sola (25 a, 25 b, 25 C)
    sufijos_pattern = r'(?:(?:bis|ter|qu[aá]ter|quartus|quintus|quinto|sextus|sexto|septimus|s[eé]ptimo|octavus|octavo|novenus|noveno|decimus|d[eé]cimo)(?:\s+\d+)?|\s*[a-zA-Z])'
    
    # Patrón para números con guión (incluye guión+número o guión+sufijo)
    numero_pattern = r'\d+(?:-\d+|-(?:bis|ter|qu[aá]ter|quartus|quintus|quinto|sextus|sexto|septimus|s[eé]ptimo|octavus|octavo|novenus|noveno|decimus|d[eé]cimo))?'
    
    article_patterns = [
        # Patrón 1: Múltiples artículos con conectores (incluye números con guión: 449-29, 63-Bis)
        (rf'\b(?:art[íi]culos?|art\.?)\s*{numero_pattern}(?:\s*[°º])?(?:\s*{sufijos_pattern})?(?:\s*(?:y|al|,)\s*{numero_pattern}(?:\s*[°º])?(?:\s*{sufijos_pattern})?)*', 'ARTICLE_MULTI'),
        
        # Patrón 2: Rangos "del 10 al 15"  
        (rf'\b(?:art[íi]culos?|art\.?)\s*(?:del\s*)?{numero_pattern}(?:\s*[°º])?(?:\s*{sufijos_pattern})?\s*al\s*{numero_pattern}(?:\s*[°º])?(?:\s*{sufijos_pattern})?', 'ARTICLE_RANGE'),
        
        # Patrón 3: Individual (respaldo)
        (rf'\b(?:art[íi]culos?|art\.?)\s*{numero_pattern}(?:\s*[°º])?(?:\s*{sufijos_pattern})?', 'ARTICLE_SINGLE'),
        
        # Patrón 4: Referencias relativas (artículo anterior, siguiente, etc.)
        # Incluye: "los dos artículos anteriores", "los cuatro artículos posteriores", etc.
        (r'\b(?:los?|las?)?\s*(?:dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce|trece|catorce|quince|veinte|\d+)?\s*(?:art[íi]culos?|art\.?)\s*(?:anterior(?:es)?|siguiente(?:s)?|precedente(?:s)?|subsecuente(?:s)?|previo(?:s)?|posterior(?:es)?|citado(?:s)?|mencionado(?:s)?)', 'ARTICLE_RELATIVE')
    ]
    
    # Grupo de patrones
    pattern_group = 'ARTICLE_MENTIONS'
    
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
                  f"Menciones encontradas: {total_matches} | Tiempo: {elapsed:.1f}s")
        
        text = str(row[text_column]) if pd.notna(row[text_column]) else ""
        article_name = row[section_column] if pd.notna(row[section_column]) else ""
        
        if not text.strip():
            continue
        
        # Evitar duplicados usando set para rastrear posiciones
        found_matches = set()
        
        # Extraer menciones de artículos
        for pattern, entity_label in article_patterns:
            try:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Crear clave única para evitar duplicados
                    match_key = (match.start(), match.end(), match.group(0))
                    if match_key in found_matches:
                        continue
                    found_matches.add(match_key)
                    
                    # Extraer 30 palabras antes y después de la coincidencia
                    context = extract_context_window(text, match.start(), match.end(), words_before=30, words_after=30)
                    
                    match_info = {
                        'doc_id': row.get('doc_id', ''),
                        'art_id': row.get('art_id', ''),
                        'document_name': row.get('document_name', ''),
                        'article_name': article_name,
                        'entity_text': match.group(0).strip(),
                        'entity_label': entity_label,
                        'pattern_group': pattern_group,
                        'before_context': context['before_context'],
                        'after_context': context['after_context'],
                        'full_context': context['full_context'],
                        'words_before_count': context['words_before_count'],
                        'words_after_count': context['words_after_count']
                    }
                    
                    results.append(match_info)
                    total_matches += 1
                    
            except re.error as e:
                print(f"Error de regex: {e}")
                continue
    
    # Resumen final
    total_time = time.time() - start_time
    total_mentions = len(results)
    
    print("=" * 70)
    print(f"EXTRACCIÓN COMPLETADA a las {datetime.now().strftime('%H:%M:%S')}")
    print(f"Tiempo total de procesamiento: {total_time:.2f} segundos")
    print(f"Documentos procesados: {processed_rows}")
    print(f"Total de menciones de artículos extraídas: {total_mentions}")
    print("=" * 70)
    
    return pd.DataFrame(results)

