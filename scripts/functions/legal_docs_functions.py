import pandas as pd
import re
import time
from datetime import datetime
from context_extraction import extract_context_window


def extract_legal_docs(df, text_column='text', section_column='article_name'):
    """    
    Extrae menciones genéricas de documentos legales:
    - Leyes genéricas (Ley + Orgánica/General/Federal/de/del/para/sobre)
    - Códigos
    - Reglamentos
    - Normas Oficiales Mexicanas (NOM)
    
    Args:
        df: DataFrame con los documentos legales
        text_column: Nombre de la columna con el texto
        section_column: Nombre de la columna con el título de la sección
    
    Returns:
        DataFrame con las menciones de documentos legales extraídas
    """
    start_time = time.time()
    results = []
    
    print(f"Iniciando extracción de DOCUMENTOS LEGALES GENÉRICOS a las {datetime.now().strftime('%H:%M:%S')}")
    
    # Patrones genéricos de documentos legales (grupo LEGAL_DOCS)
    legal_patterns = [
        (r'\b(?:Ley|LEY)\s+(?:Orgánica|General|Federal|de|del|para|sobre)\s+[A-ZÁÉÍÓÚÑa-záéíóúñ\s]{8,150}', 'LAW_MENTION'),
        (r'\b(?:Código|CÓDIGO)\s+(?:de|del|para)\s+[A-ZÁÉÍÓÚÑa-záéíóúñ\s]{10,80}', 'LAW_CODE'),
        (r'\b(?:Reglamento|REGLAMENTO)\s+(?:de|del|para)\s+[A-ZÁÉÍÓÚÑa-záéíóúñ\s]{10,100}', 'REGULATION'),
        (r'\b(?:Norma|NORMA)\s+Oficial\s+Mexicana\s+[A-Z0-9\-]+', 'NOM')
    ]
    
    # Grupo de patrones
    pattern_group = 'LEGAL_DOCS'
    
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
                  f"Documentos encontrados: {total_matches} | Tiempo: {elapsed:.1f}s")
        
        text = str(row[text_column]) if pd.notna(row[text_column]) else ""
        article_name = row[section_column] if pd.notna(row[section_column]) else ""
        
        if not text.strip():
            continue
        
        # Extraer menciones de documentos legales
        for pattern, entity_label in legal_patterns:
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
    print(f"Total de documentos legales extraídos: {total_mentions}")
    print("=" * 70)
    
    return pd.DataFrame(results)

