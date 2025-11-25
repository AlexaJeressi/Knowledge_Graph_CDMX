import re


def extract_context_window(text, match_start, match_end, words_before=30, words_after=30):
    """
    Extrae una ventana de contexto alrededor de una coincidencia con un número específico
    de palabras antes y después.
    
    Esta función es utilizada por todos los scripts de extracción de entidades
    para capturar el contexto alrededor de cada mención encontrada.
    
    Args:
        text: Texto completo
        match_start: Posición inicial de la coincidencia
        match_end: Posición final de la coincidencia
        words_before: Número de palabras a incluir antes de la coincidencia (default: 30)
        words_after: Número de palabras a incluir después de la coincidencia (default: 30)
    
    Returns:
        dict: Diccionario con la siguiente estructura:
            - before_context: Texto antes de la coincidencia
            - matched_entity: Texto de la coincidencia
            - after_context: Texto después de la coincidencia
            - full_context: Contexto completo con la coincidencia marcada
            - words_before_count: Número real de palabras antes
            - words_after_count: Número real de palabras después
    
    Example:
        >>> context = extract_context_window(text, 100, 120, words_before=10, words_after=10)
        >>> print(context['full_context'])
    """
    # Dividir texto en palabras usando regex (maneja mejor la puntuación)
    words = re.findall(r'\b\w+\b|\S', text)
    
    # Encontrar posiciones de palabras para la coincidencia
    char_to_word = {}
    char_pos = 0
    
    for word_idx, word in enumerate(words):
        word_start = text.find(word, char_pos)
        word_end = word_start + len(word)
        
        for char_idx in range(word_start, word_end):
            char_to_word[char_idx] = word_idx
            
        char_pos = word_end
    
    # Encontrar índices de palabras para los límites de la coincidencia
    match_start_word = char_to_word.get(match_start, 0)
    match_end_word = char_to_word.get(match_end - 1, len(words) - 1)
    
    # Calcular límites del contexto
    context_start = max(0, match_start_word - words_before)
    context_end = min(len(words), match_end_word + words_after + 1)
    
    # Extraer palabras del contexto
    before_words = words[context_start:match_start_word]
    match_words = words[match_start_word:match_end_word + 1]
    after_words = words[match_end_word + 1:context_end]
    
    # Unir de vuelta a texto
    before_text = ' '.join(before_words) if before_words else ""
    match_text = ' '.join(match_words)
    after_text = ' '.join(after_words) if after_words else ""
    
    # Crear contexto completo con marcador
    full_context = f"{before_text} **{match_text}** {after_text}".strip()
    
    return {
        'before_context': before_text.strip(),
        'matched_entity': match_text.strip(), 
        'after_context': after_text.strip(),
        'full_context': full_context,
        'words_before_count': len(before_words),
        'words_after_count': len(after_words)
    }

