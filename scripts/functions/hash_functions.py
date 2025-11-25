"""
Funciones para la creación de identificadores únicos (hashes) para documentos legales
"""

import re
import hashlib
import pandas as pd
import unicodedata


def create_document_hash(document_name):
    """
    Crea un hash corto de 8 caracteres a partir del nombre del documento
    
    Args:
        document_name (str): Nombre del documento
        
    Returns:
        str: Hash de 8 caracteres en mayúsculas
    """
    if pd.isna(document_name):
        return "UNKNOWN"
    # Crear hash MD5 y tomar los primeros 8 caracteres
    hash_obj = hashlib.md5(str(document_name).encode('utf-8'))
    return hash_obj.hexdigest()[:8].upper()


def normalize_text_for_hash(text):
    """
    Normaliza texto para crear hashes consistentes
    
    Args:
        text (str): Texto a normalizar
        
    Returns:
        str: Texto normalizado
    """
    if not text or text.strip() == '':
        return "EMPTY"
    
    # 1. Convertir a mayúsculas para consistencia
    text = text.upper().strip()
    
    # 2. Remover acentos y caracteres especiales
    text = unicodedata.normalize('NFD', text)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
    
    # 3. Remover espacios extra y caracteres no alfanuméricos
    text = re.sub(r'[^A-Z0-9]', '', text)
    
    # 4. Remover palabras comunes que pueden variar
    common_words = ['LA', 'EL', 'DE', 'DEL', 'LOS', 'LAS', 'UN', 'UNA']
    words = text.split()
    words = [word for word in words if word not in common_words]
    text = ''.join(words)
    
    return text if text else "EMPTY"


def remove_accents(text):
    """
    Elimina acentos y diacríticos de un texto, manteniendo espacios y otros caracteres.
    
    Args:
        text (str): Texto con acentos
        
    Returns:
        str: Texto sin acentos
        
    Examples:
        >>> remove_accents("México")
        "Mexico"
        >>> remove_accents("Ley de Educación Física")
        "Ley de Educacion Fisica"
    """
    if pd.isna(text) or text is None:
        return text
    
    # Normalizar a NFD (Decomposed form) para separar caracteres base de diacríticos
    text_nfd = unicodedata.normalize('NFD', str(text))
    
    # Filtrar solo los caracteres que NO son marcas diacríticas (categoría Mn)
    text_sin_acentos = ''.join(char for char in text_nfd if unicodedata.category(char) != 'Mn')
    
    # Re-normalizar a NFC (Composed form) para normalización Unicode estándar
    return unicodedata.normalize('NFC', text_sin_acentos)


def clean_section_title(section_title, max_length=13):
    """
    Limpia el título de una sección para usarlo como identificador
    
    Args:
        section_title (str): Título de la sección
        max_length (int): Longitud máxima del identificador (por defecto 13)
        
    Returns:
        str: Título limpio en mayúsculas
    """
    if pd.isna(section_title):
        return "NOSEC"
    
    # Remover caracteres no alfanuméricos y limitar longitud
    clean_title = re.sub(r'[^a-zA-Z0-9]', '', str(section_title))[:max_length].upper()
    
    return clean_title if clean_title else "NOSEC"



