#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper de leyes (multi-URL):
- Acepta N URLs directamente por CLI o con --from-file urls.txt (una por línea).
- Combina TODO en un solo CSV con columnas:
  nombre, fecha_publicacion, fecha_actualizacion, link_pdf, source_url
- Descarga todos los DOCX (si existen) a docx_leyes/
- Evita duplicados por (nombre, link_pdf)
"""
from __future__ import annotations
import sys, csv, re, os, time, argparse, unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, Iterable, List, Set
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

SPANISH_MONTHS = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6,
    "JULIO": 7, "AGOSTO": 8, "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10,
    "NOVIEMBRE": 11, "DICIEMBRE": 12
}
def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
def to_iso_date(day: int, month: int, year: int) -> str:
    try:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    except Exception:
        return ""
def parse_spanish_date(text: str) -> Optional[str]:
    if not text: return None
    t = strip_accents(text.upper())
    m = re.search(r"(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÑ]+)\s+DE\s+(\d{4})", t, re.IGNORECASE)
    if m:
        d = int(m.group(1)); mon_name = strip_accents(m.group(2).upper()); y = int(m.group(3))
        mon = SPANISH_MONTHS.get(mon_name, 0)
        if mon: return to_iso_date(d, mon, y)
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", t)
    if m:
        d, mon, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100: y += 2000 if y < 50 else 1900
        return to_iso_date(d, mon, y)
    return None
def extract_dates(meta_text: str):
    if not meta_text: return None, None
    text = norm_spaces(meta_text); up = text.upper()
    pub_date = None
    m = re.search(r"PUBLICADO[A-Z\s]*EL\s+(.*?)(?:\.|;|$)", up, flags=re.IGNORECASE)
    if m: pub_date = parse_spanish_date(m.group(1))
    upd_date = None
    m2 = re.search(r"(ULTIMA|ÚLTIMA)\s+REFORMA[^\d]*([^.]+)", up, flags=re.IGNORECASE)
    if m2: upd_date = parse_spanish_date(m2.group(2))
    else:
        m3 = re.search(r"ACTUALIZACI[OÓ]N[^\d]*([^.]+)", up, flags=re.IGNORECASE)
        if m3: upd_date = parse_spanish_date(m3.group(1))
    if not pub_date:
        first_any = parse_spanish_date(up)
        if first_any: pub_date = first_any
    return pub_date, upd_date
def safe_filename(s: str) -> str:
    s = strip_accents(s); s = re.sub(r"[^\w\s.-]", "", s); s = re.sub(r"\s+", "_", s).strip("_")
    return s[:150] if s else "archivo"
@dataclass
class LawItem:
    nombre: str
    fecha_publicacion: Optional[str]
    fecha_actualizacion: Optional[str]
    link_pdf: str
    source_url: str
    link_docx: Optional[str]
def fetch(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"}
    resp = requests.get(url, headers=headers, timeout=30); resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"; return resp.text
def parse_page(html: str, base_url: str) -> List[LawItem]:
    soup = BeautifulSoup(html, "html.parser"); items: List[LawItem] = []
    for item in soup.select("div.art-article div.nn_sliders_item"):
        title_el = item.select_one("h2.nn_sliders_title")
        nombre = norm_spaces(title_el.get_text(strip=True)) if title_el else ""
        meta_td = item.select_one("table tbody tr td[style*='width: 70%']"); meta_text = ""
        if meta_td: meta_text = " ".join(norm_spaces(p.get_text(' ', strip=True)) for p in meta_td.select("p"))
        fecha_pub, fecha_upd = extract_dates(meta_text)
        pdf_a = item.select_one("a[href$='.pdf'], a[href$='.PDF']"); docx_a = item.select_one("a[href$='.docx'], a[href$='.DOCX']")
        link_pdf = urljoin(base_url, pdf_a["href"]) if pdf_a and pdf_a.has_attr("href") else ""
        link_docx = urljoin(base_url, docx_a["href"]) if docx_a and docx_a.has_attr("href") else None
        items.append(LawItem(nombre, fecha_pub, fecha_upd, link_pdf, base_url, link_docx))
    if not items:
        for box in soup.select("div.blog div.art-post, div.art-post"):
            title_el = box.select_one("h2.nn_sliders_title, h2")
            nombre = norm_spaces(title_el.get_text(strip=True)) if title_el else ""
            meta_td = box.select_one("table tbody tr td[style*='width: 70%']"); meta_text = ""
            if meta_td: meta_text = " ".join(norm_spaces(p.get_text(' ', strip=True)) for p in meta_td.select("p"))
            fecha_pub, fecha_upd = extract_dates(meta_text)
            pdf_a = box.select_one("a[href$='.pdf'], a[href$='.PDF']"); docx_a = box.select_one("a[href$='.docx'], a[href$='.DOCX']")
            link_pdf = urljoin(base_url, pdf_a["href"]) if pdf_a and pdf_a.has_attr("href") else ""
            link_docx = urljoin(base_url, docx_a["href"]) if docx_a and docx_a.has_attr("href") else None
            if nombre or link_pdf or link_docx:
                items.append(LawItem(nombre, fecha_pub, fecha_upd, link_pdf, base_url, link_docx))
    return items
def download_docx(url: str, out_dir: Path, name_hint: str) -> Optional[Path]:
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        fn = safe_filename(name_hint) or "documento"
        tail = os.path.basename(urlparse(url).path)
        if tail.lower().endswith(".docx"):
            base = os.path.splitext(tail)[0]; fn = safe_filename(base) or fn
        outfile = out_dir / f"{fn}.docx"
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(outfile, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk: f.write(chunk)
        return outfile
    except Exception as e:
        print(f"[WARN] No se pudo descargar DOCX {url}: {e}")
        return None
def download_pdf(url: str, out_dir: Path, name_hint: str) -> Optional[Path]:
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        fn = safe_filename(name_hint) or "documento"
        tail = os.path.basename(urlparse(url).path)
        if tail.lower().endswith(".pdf"):
            base = os.path.splitext(tail)[0]; fn = safe_filename(base) or fn
        outfile = out_dir / f"{fn}.pdf"
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(outfile, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk: f.write(chunk)
        return outfile
    except Exception as e:
        print(f"[WARN] No se pudo descargar PDF {url}: {e}")
        return None
def iter_urls(args):
    if args.from_file:
        path = Path(args.from_file)
        for line in path.read_text(encoding="utf-8").splitlines():
            u = line.strip()
            if u and not u.startswith("#"): yield u
    for u in args.urls or []: yield u
def main():
    ap = argparse.ArgumentParser(description="Scraper de leyes (multi-URL)")
    ap.add_argument("urls", nargs="*", help="Una o más URLs a procesar")
    ap.add_argument("--from-file", help="Archivo de texto con URLs (una por línea)")
    ap.add_argument("--out", default="leyes.csv", help="CSV de salida (default: leyes.csv)")
    ap.add_argument("--sleep", type=float, default=0.5, help="Segundos entre requests")
    args = ap.parse_args()
    urls = list(iter_urls(args))
    if not urls: ap.error("Proporciona al menos una URL o usa --from-file urls.txt")
    docx_dir = Path("docx_leyes"); out_csv = Path(args.out)
    all_items: List[LawItem] = []; seen: Set[tuple] = set()
    for u in urls:
        print(f"[i] Descargando: {u}")
        html = fetch(u); items = parse_page(html, u)
        print(f"    {len(items)} registros en esta página.")
        for it in items:
            key = (it.nombre, it.link_pdf)
            if key in seen: continue
            seen.add(key); all_items.append(it)
            if it.link_docx:
                saved = download_docx(it.link_docx, docx_dir, it.nombre or "ley")
                if saved: print(f"    DOCX guardado: {saved.name}")
                time.sleep(args.sleep)
            elif it.link_pdf:
                saved = download_pdf(it.link_pdf, docx_dir, it.nombre or "ley")
                if saved: print(f"    PDF guardado (no había DOCX): {saved.name}")
                time.sleep(args.sleep)
        time.sleep(args.sleep)
    print(f"[i] Total combinado: {len(all_items)} registros. Guardando CSV en {out_csv}")
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f); w.writerow(["nombre","fecha_publicacion","fecha_actualizacion","link_pdf","link_docx","source_url"])
        for it in all_items: w.writerow([it.nombre, it.fecha_publicacion or "", it.fecha_actualizacion or "", it.link_pdf, it.link_docx or "", it.source_url])
    print("[✔] Listo.")
if __name__ == "__main__":
    main()
