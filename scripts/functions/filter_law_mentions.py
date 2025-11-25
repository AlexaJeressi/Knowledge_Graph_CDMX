# Algoritmo para filtrar menciones de leyes reales vs menciones vagas usando LLM
import re
import pandas as pd
from typing import Tuple, Dict
import json
from openai import OpenAI
import os
from time import sleep

def has_proper_law_capitalization(text: str) -> bool:
    """
    Verifica si el texto tiene capitalización apropiada para un nombre de ley.
    
    Args:
        text (str): Texto a analizar
    
    Returns:
        bool: True si tiene capitalización apropiada, False en caso contrario
    """
    if not text or len(text.strip()) == 0:
        return False
    
    text = text.strip()
    words = text.split()
    
    if len(words) < 2:  # Muy corto, probablemente no es nombre completo
        return False
    
    # Primera palabra debe ser "Ley" o "LEY"
    if not words[0].startswith(('Ley', 'LEY')):
        return False
    
    # Segunda palabra debe ser una preposición común
    if words[1].lower() not in ['de', 'del', 'para', 'sobre', 'que']:
        return False
    
    # Al menos una palabra después de la preposición debe estar capitalizada
    capitalized_words = 0
    for word in words[2:]:
        if len(word) > 0 and word[0].isupper():
            capitalized_words += 1
        # Palabras que siempre van en minúscula en nombres oficiales no cuentan
        if word.lower() in ['de', 'del', 'la', 'el', 'y', 'en', 'para', 'con', 'al', 'los', 'las', 'Federal', 'de los', 'de las', 'General']:
            continue
    
    # Debe tener al menos 2 palabras capitalizadas después de la preposición
    return capitalized_words >= 2

def classify_law_mention_with_llm(text: str, client: OpenAI) -> Dict:
    """
    Usa un LLM (ChatGPT) para determinar si un texto es un nombre oficial de ley
    y extraer el nombre correcto si lo es.
    
    Args:
        text (str): Texto a analizar
        client (OpenAI): Cliente de OpenAI inicializado
    
    Returns:
        dict: {"is_official": bool, "official_name": str, "reasoning": str}
    """
    prompt = f"""Analiza el siguiente texto que fue extraído de un documento legal mexicano y determina:
1. ¿Es un nombre oficial de una ley? (sí o no)
2. Si es oficial, extrae el nombre completo y correcto de la ley
3. Proporciona una breve razón de tu decisión

Criterios para ser un nombre oficial:
- Debe ser un nombre formal de una ley (no una descripción o mención vaga)
- Puede estar en mayúsculas, minúsculas o capitalizado
- Ejemplos de nombres oficiales: "Ley de Salud", "LEY DE TRANSPARENCIA", "Ley para la Protección de Datos"
- NO son nombres oficiales: descripciones largas, menciones vagas como "la ley", "ley aplicable", texto con verbos descriptivos

Texto a analizar: "{text}"

Responde ÚNICAMENTE en formato JSON con esta estructura exacta:
{{
    "is_official": true/false,
    "official_name": "nombre oficial si aplica, o cadena vacía si no es oficial",
    "reasoning": "breve explicación"
}}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un experto en identificar nombres oficiales de leyes mexicanas. Respondes únicamente en formato JSON válido."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=300
        )
        
        # Extraer y parsear la respuesta
        content = response.choices[0].message.content.strip()
        
        # Limpiar markdown si existe
        if content.startswith("```json"):
            content = content.replace("```json", "").replace("```", "").strip()
        
        result = json.loads(content)
        return result
        
    except Exception as e:
        print(f"Error al procesar '{text[:50]}...': {e}")
        return {
            "is_official": False,
            "official_name": "",
            "reasoning": f"Error: {str(e)}"
        }

def filter_law_mentions_with_llm(df: pd.DataFrame, entity_text_col: str = 'entity_text', 
                                  api_key: str = None, batch_size: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filtra las menciones de leyes usando un LLM (ChatGPT) para identificar nombres oficiales.
    
    Args:
        df (pd.DataFrame): DataFrame con menciones de leyes
        entity_text_col (str): Nombre de la columna con el texto de la entidad
        api_key (str): API key de OpenAI (si None, se toma de variable de entorno)
        batch_size (int): Número de registros a procesar antes de hacer una pausa
    
    Returns:
        tuple: (nombres_oficiales_df, menciones_texto_df)
    """
    print("=== FILTRADO DE MENCIONES DE LEYES CON LLM ===")
    print(f"Total de menciones a analizar: {len(df)}")
    
    # Inicializar cliente de OpenAI
    if api_key is None:
        api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key:
        raise ValueError("Se requiere OPENAI_API_KEY. Configúrala como variable de entorno o pásala como parámetro.")
    
    client = OpenAI(api_key=api_key)
    
    # Procesar cada mención
    results = []
    for idx, row in df.iterrows():
        text = row[entity_text_col]
        
        # Clasificar con LLM
        classification = classify_law_mention_with_llm(text, client)
        
        results.append({
            'is_official': classification['is_official'],
            'official_name': classification['official_name'],
            'reasoning': classification['reasoning']
        })
        
        # Mostrar progreso
        if (idx + 1) % 10 == 0:
            print(f"Procesados: {idx + 1}/{len(df)}")
        
        # Pausa para evitar rate limits
        if (idx + 1) % batch_size == 0:
            sleep(1)
    
    # Agregar resultados al DataFrame
    df_results = df.copy()
    df_results['is_official'] = [r['is_official'] for r in results]
    df_results['official_name'] = [r['official_name'] for r in results]
    df_results['llm_reasoning'] = [r['reasoning'] for r in results]
    
    # Separar en dos DataFrames
    nombres_oficiales = df_results[df_results['is_official'] == True].copy()
    menciones_texto = df_results[df_results['is_official'] == False].copy()
    
    # Actualizar entity_text con el nombre oficial extraído
    if len(nombres_oficiales) > 0:
        nombres_oficiales[entity_text_col] = nombres_oficiales['official_name']
    
    # Eliminar columnas auxiliares del output final (opcional, puedes mantenerlas para análisis)
    # nombres_oficiales = nombres_oficiales.drop(['is_official', 'official_name', 'llm_reasoning'], axis=1)
    # menciones_texto = menciones_texto.drop(['is_official', 'official_name', 'llm_reasoning'], axis=1)
    
    # Estadísticas
    print(f"\n=== RESULTADOS ===")
    print(f"Nombres oficiales identificados: {len(nombres_oficiales)}")
    print(f"Menciones filtradas: {len(menciones_texto)}")
    print(f"Tasa de filtrado: {len(menciones_texto)/len(df)*100:.1f}%")
    
    # Mostrar ejemplos de cada categoría
    print(f"\n=== EJEMPLOS DE NOMBRES OFICIALES ===")
    for i, (text, official) in enumerate(zip(nombres_oficiales[entity_text_col].head(10), 
                                               nombres_oficiales['official_name'].head(10))):
        print(f"{i+1}. Original: {text}")
        if text != official:
            print(f"   Normalizado: {official}")
    
    print(f"\n=== EJEMPLOS DE MENCIONES FILTRADAS ===")
    for i, (text, reason) in enumerate(zip(menciones_texto[entity_text_col].head(10),
                                            menciones_texto['llm_reasoning'].head(10))):
        print(f"{i+1}. {text}")
        print(f"   Razón: {reason}")
    
    return nombres_oficiales, menciones_texto

def filter_law_mentions_by_capitalization(df: pd.DataFrame, entity_text_col: str = 'entity_text') -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filtra las menciones de leyes separando nombres oficiales de menciones en texto
    usando únicamente patrones de capitalización.
    
    Args:
        df (pd.DataFrame): DataFrame con menciones de leyes
        entity_text_col (str): Nombre de la columna con el texto de la entidad
    
    Returns:
        tuple: (nombres_oficiales_df, menciones_texto_df)
    """
    print("=== FILTRADO DE MENCIONES DE LEYES POR CAPITALIZACIÓN ===")
    print(f"Total de menciones a analizar: {len(df)}")
    
    # Aplicar filtro de capitalización
    df['is_likely_law_name'] = df[entity_text_col].apply(has_proper_law_capitalization)
    
    # Separar en dos DataFrames
    nombres_oficiales = df[df['is_likely_law_name'] == True].copy()
    menciones_texto = df[df['is_likely_law_name'] == False].copy()
    
    # Eliminar columna auxiliar
    nombres_oficiales = nombres_oficiales.drop('is_likely_law_name', axis=1)
    menciones_texto = menciones_texto.drop('is_likely_law_name', axis=1)
    
    # Estadísticas
    print(f"Nombres oficiales identificados: {len(nombres_oficiales)}")
    print(f"Menciones en texto filtradas: {len(menciones_texto)}")
    print(f"Tasa de filtrado: {len(menciones_texto)/len(df)*100:.1f}%")
    
    # Mostrar ejemplos de cada categoría
    print(f"\n=== EJEMPLOS DE NOMBRES OFICIALES ===")
    for i, text in enumerate(nombres_oficiales[entity_text_col].head(10)):
        print(f"{i+1}. {text}")
    
    print(f"\n=== EJEMPLOS DE MENCIONES FILTRADAS ===")
    for i, text in enumerate(menciones_texto[entity_text_col].head(10)):
        print(f"{i+1}. {text}")
    
    return nombres_oficiales, menciones_texto

