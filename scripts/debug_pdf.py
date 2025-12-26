"""Script de debug pour analyser ligne par ligne"""

import pdfplumber

pdf_path = "data/pdfs/devis_d202501-001_rapido_devis.pdf"

with pdfplumber.open(pdf_path) as pdf:
    text = pdf.pages[0].extract_text()
    lines = text.split('\n')
    
    print("LIGNES DU PDF (numérotées):")
    print("="*60)
    for i, line in enumerate(lines[:20]):  # 20 premières lignes
        print(f"{i:2d} | {repr(line)}")