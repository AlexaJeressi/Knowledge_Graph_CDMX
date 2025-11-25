import os
import zipfile
import tempfile
import shutil
from xml.etree import ElementTree as ET
import argparse

def _zipdir(path, ziph):
    for root, _, files in os.walk(path):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, path)
            ziph.write(full_path, rel_path)

def clear_headers_footers_in_docx(in_path, out_path):
    """
    Abre el .docx como zip, borra el contenido de header*.xml/footer*.xml y vuelve a empacar.
    """
    tmpdir = tempfile.mkdtemp()
    modified = []
    try:
        with zipfile.ZipFile(in_path, 'r') as zin:
            zin.extractall(tmpdir)

        word_dir = os.path.join(tmpdir, "word")
        if os.path.isdir(word_dir):
            for name in os.listdir(word_dir):
                if name.startswith(("header", "footer")) and name.endswith(".xml"):
                    xml_path = os.path.join(word_dir, name)
                    try:
                        tree = ET.parse(xml_path)
                        root = tree.getroot()
                        # Borra todos los hijos (deja el nodo raíz + namespaces)
                        for child in list(root):
                            root.remove(child)
                        tree.write(xml_path, encoding="utf-8", xml_declaration=True)
                        modified.append(name)
                    except Exception as e:
                        modified.append(f"{name} (ERROR: {e})")

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with zipfile.ZipFile(out_path, 'w', compression=zipfile.ZIP_DEFLATED) as zout:
            _zipdir(tmpdir, zout)
        return modified
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def process_folder(input_dir):
    total = 0
    changed = 0
    errors = 0
    skipped_pdf = 0
    
    for root, _, files in os.walk(input_dir):
        for f in files:
            # Saltar PDFs
            if f.lower().endswith(".pdf"):
                skipped_pdf += 1
                continue
            
            # Procesar solo DOCX
            if not f.lower().endswith(".docx"):
                continue
                
            total += 1
            in_path = os.path.join(root, f)
            rel = os.path.relpath(in_path, input_dir)
            
            # Crear archivo temporal para el resultado
            temp_path = in_path + ".tmp"
            
            try:
                mods = clear_headers_footers_in_docx(in_path, temp_path)
                # Reemplazar el original con el temporal
                shutil.move(temp_path, in_path)
                changed += 1
                print(f"[OK] {rel}  -> headers/footers limpiados: {', '.join(mods) if mods else 'ninguno encontrado'}")
            except Exception as e:
                errors += 1
                print(f"[ERROR] {rel}: {e}")
                # Limpiar archivo temporal si existe
                if os.path.exists(temp_path):
                    os.remove(temp_path)

    print("\nResumen:")
    print(f"  Documentos .docx encontrados: {total}")
    print(f"  Procesados con éxito:        {changed}")
    print(f"  Con errores:                 {errors}")
    print(f"  PDFs ignorados:              {skipped_pdf}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quitar headers/footers de .docx en lote (sustituye los originales).")
    parser.add_argument("input_dir", help="Carpeta con archivos .docx y .pdf (los PDF se ignoran)")
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        raise SystemExit(f"La carpeta de entrada no existe: {args.input_dir}")

    print(f"ADVERTENCIA: Los archivos .docx en '{args.input_dir}' serán MODIFICADOS (sustituidos).")
    print("Los archivos .pdf serán ignorados.\n")
    
    process_folder(args.input_dir)
