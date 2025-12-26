"""
RAPIDO'DEVIS - PDF PARSER
Extraction automatique des données de devis au format PDF
"""

import re
import pdfplumber
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import hashlib


class RapidoDevisParser:
    """Parser intelligent pour les PDFs de devis Rapido'Devis"""
    
    def __init__(self, pdf_path: str):
        self.pdf_path = Path(pdf_path)
        self.data = {}
        
    def parse(self) -> Dict:
        """Parse complet du PDF et retourne un dictionnaire structuré"""
        
        with pdfplumber.open(self.pdf_path) as pdf:
            first_page = pdf.pages[0]
            text_content = first_page.extract_text()
            
            self.data = {
                'metadata': self._extract_metadata(text_content),
                'client': self._extract_client_info(text_content),
                'chantier': self._extract_chantier_info(text_content),
                'pieces': [],
                'lignes': [],
                'montants': {},
                'pdf_info': {
                    'path': str(self.pdf_path),
                    'hash': self._calculate_pdf_hash()
                }
            }
            
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    self._parse_table(table)
            
            last_page_text = pdf.pages[-1].extract_text()
            self.data['montants'] = self._extract_montants(last_page_text)
            
        return self.data
    
    def _extract_metadata(self, text: str) -> Dict:
        """Extrait numéro de devis, date, etc."""
        metadata = {}
        
        num_match = re.search(r'N°\s*(D\d{6}-\d+)', text)
        if num_match:
            metadata['numero_devis'] = num_match.group(1)
        
        date_match = re.search(r'En date du\s*(\d{2}/\d{2}/\d{4})', text)
        if date_match:
            date_str = date_match.group(1)
            metadata['date_devis'] = datetime.strptime(date_str, '%d/%m/%Y').date()
        
        valide_match = re.search(r'valable jusqu\'au\s*(\d{2}/\d{2}/\d{4})', text)
        if valide_match:
            date_str = valide_match.group(1)
            metadata['date_validite'] = datetime.strptime(date_str, '%d/%m/%Y').date()
        
        return metadata
    
    def _extract_client_info(self, text: str) -> Dict:
        """Extrait les informations du client"""
        client = {}
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Pattern: "XXX Mme/M. Prénom Nom"
            match_avec_prefix = re.search(r'(Mme|M\.|Monsieur|Madame)\s+([A-ZÀ-Ÿ][a-zà-ÿ]+(?:\s+[A-ZÀ-Ÿ][a-zà-ÿ]+)+)', line_clean)
            if match_avec_prefix:
                client['nom'] = match_avec_prefix.group(2).strip()
                
                for j in range(i+1, min(i+6, len(lines))):
                    next_line = lines[j].strip()
                    
                    if not next_line or 'SIRET' in next_line or 'RAPIDO' in next_line:
                        continue
                    
                    if not client.get('adresse') and re.match(r'^\d+\s+', next_line):
                        client['adresse'] = next_line
                        continue
                    
                    if re.match(r'^\d{5}\s+', next_line):
                        parts = next_line.split(maxsplit=1)
                        client['code_postal'] = parts[0]
                        if len(parts) > 1:
                            client['ville'] = parts[1]
                        break
                break
            
            elif re.match(r'^(Mme|M\.|Monsieur|Madame)\s+[A-ZÀ-Ÿ]', line_clean):
                nom_complet = re.sub(r'^(Mme|M\.|Monsieur|Madame)\s+', '', line_clean)
                client['nom'] = nom_complet
                
                if i+1 < len(lines) and re.search(r'\d', lines[i+1]):
                    client['adresse'] = lines[i+1].strip()
                if i+2 < len(lines) and re.search(r'\d{5}', lines[i+2]):
                    parts = lines[i+2].strip().split()
                    if len(parts) >= 2:
                        client['code_postal'] = parts[0]
                        client['ville'] = ' '.join(parts[1:])
                break
            
            elif re.match(r'^[A-ZÀ-Ÿ][a-zà-ÿ]+(\s+[A-ZÀ-Ÿ][a-zà-ÿ]+)+$', line_clean):
                client['nom'] = line_clean
                
                if i+1 < len(lines):
                    client['adresse'] = lines[i+1].strip()
                if i+2 < len(lines) and re.search(r'\d{5}', lines[i+2]):
                    parts = lines[i+2].strip().split()
                    if len(parts) >= 2:
                        client['code_postal'] = parts[0]
                        client['ville'] = ' '.join(parts[1:])
                break
        
        client['type_client'] = 'PARTICULIER'
        return client
    
    def _extract_chantier_info(self, text: str) -> Dict:
        """Extrait l'adresse du chantier"""
        chantier = {}
        
        match = re.search(r'Adresse du chantier\s*([^\n]+)\s*([^\n]*)\s*(\d{5})\s*([^\n]+)', text, re.MULTILINE)
        
        if match:
            chantier['adresse'] = match.group(1).strip()
            if match.group(2).strip().startswith('('):
                chantier['complement'] = match.group(2).strip()
            chantier['code_postal'] = match.group(3)
            chantier['ville'] = match.group(4).strip()
        
        return chantier
    
    def _parse_table(self, table: List[List]) -> None:
        """Parse un tableau de lignes de devis"""
        if not table or len(table) < 2:
            return
        
        current_piece = None
        current_category = None
        
        for row in table:
            if not row or not row[0]:
                continue
            
            line = str(row[0]).strip()
            
            if 'DÉSIGNATION' in line or not line:
                continue
            
            piece_match = re.match(r'^(\d+)\s+(.+?)\s*-\s*([\d.]+)\s*m²', line)
            if piece_match:
                piece_num = piece_match.group(1)
                piece_name = piece_match.group(2).strip()
                piece_surface = float(piece_match.group(3))
                
                current_piece = {
                    'numero': piece_num,
                    'nom': piece_name,
                    'surface': piece_surface
                }
                if not any(p['numero'] == piece_num for p in self.data['pieces']):
                    self.data['pieces'].append(current_piece)
                continue
            
            cat_match = re.match(r'^(\d+\.\d+)\s+([A-ZÀÂÄÉÈÊËÏÎÔÙÛÜŸŒÆÇ][a-zàâäéèêëïîôùûüÿœæç\s/]+)$', line)
            if cat_match:
                current_category = cat_match.group(2).strip()
                continue
            
            ligne_match = re.match(
                r'^(\d+\.\d+\.\d+)\s+(.+?)\s+(\d+(?:[.,]\d+)?)\s+(\w+)\s+([\d,]+)\s*€\s+([\d.]+)\s*%\s+([\d,]+)\s*€',
                line
            )
            
            if ligne_match:
                numero = ligne_match.group(1)
                designation_et_desc = ligne_match.group(2).strip()
                quantite = float(ligne_match.group(3).replace(',', '.'))
                unite = ligne_match.group(4)
                prix_unitaire = float(ligne_match.group(5).replace(',', '.'))
                tva = float(ligne_match.group(6))
                total_ht = float(ligne_match.group(7).replace(',', '.'))
                
                parts = designation_et_desc.split('\n', 1)
                designation = parts[0].strip()
                description = parts[1].strip() if len(parts) > 1 else ''
                
                ligne = {
                    'numero_ligne': numero,
                    'piece': current_piece['nom'] if current_piece else None,
                    'surface_piece': current_piece['surface'] if current_piece else None,
                    'categorie': current_category,
                    'designation': designation,
                    'description': description,
                    'quantite': quantite,
                    'unite': unite,
                    'prix_unitaire_ht': prix_unitaire,
                    'taux_tva': tva,
                    'total_ht': total_ht
                }
                
                self.data['lignes'].append(ligne)
    
    def _extract_montants(self, text: str) -> Dict:
        """Extrait les montants totaux du devis"""
        montants = {}
        
        ht_match = re.search(r'Total net HT\s*([\d\s,]+)\s*€', text)
        if ht_match:
            montants['total_ht'] = float(ht_match.group(1).replace(' ', '').replace(',', '.'))
        
        tva10_match = re.search(r'TVA\s*\(10\.0%\)\s*([\d\s,]+)\s*€', text)
        if tva10_match:
            montants['tva_10_pct'] = float(tva10_match.group(1).replace(' ', '').replace(',', '.'))
        
        tva20_match = re.search(r'TVA\s*\(20\.0%\)\s*([\d\s,]+)\s*€', text)
        if tva20_match:
            montants['tva_20_pct'] = float(tva20_match.group(1).replace(' ', '').replace(',', '.'))
        
        ttc_match = re.search(r'Total TTC\s*([\d\s,]+)\s*€', text)
        if ttc_match:
            montants['total_ttc'] = float(ttc_match.group(1).replace(' ', '').replace(',', '.'))
        
        if self.data.get('pieces'):
            montants['surface_totale'] = sum(p['surface'] for p in self.data['pieces'])
        
        return montants
    
    def _calculate_pdf_hash(self) -> str:
        """Calcule le hash SHA256 du PDF pour détecter les doublons"""
        sha256_hash = hashlib.sha256()
        with open(self.pdf_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convertit les lignes en DataFrame pandas"""
        if not self.data.get('lignes'):
            return pd.DataFrame()
        
        df = pd.DataFrame(self.data['lignes'])
        df['numero_devis'] = self.data['metadata'].get('numero_devis')
        df['date_devis'] = self.data['metadata'].get('date_devis')
        df['client_nom'] = self.data['client'].get('nom')
        
        return df
    
    def to_json(self) -> Dict:
        """Export JSON complet"""
        return self.data


# ============================================
# SCRIPT DE TEST
# ============================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    else:
        pdf_dir = Path("data/pdfs")
        pdfs = list(pdf_dir.glob("*.pdf"))
        if not pdfs:
            print("❌ Aucun PDF trouvé dans data/pdfs/")
            sys.exit(1)
        pdf_path = pdfs[0]
    
    print(f"🔍 Test du parser sur : {Path(pdf_path).name}\n")
    
    parser = RapidoDevisParser(pdf_path)
    data = parser.parse()
    
    print("=== MÉTADONNÉES ===")
    print(f"Numéro: {data['metadata'].get('numero_devis')}")
    print(f"Date: {data['metadata'].get('date_devis')}")
    
    print("\n=== CLIENT ===")
    print(f"Nom: {data['client'].get('nom')}")
    print(f"Ville: {data['client'].get('ville')}")
    
    print("\n=== CHANTIER ===")
    print(f"Adresse: {data['chantier'].get('adresse')}")
    print(f"Ville: {data['chantier'].get('ville')}")
    
    print("\n=== PIÈCES ===")
    for piece in data['pieces']:
        print(f"- {piece['nom']}: {piece['surface']} m²")
    
    print("\n=== MONTANTS ===")
    print(f"Total HT: {data['montants'].get('total_ht', 0):.2f} €")
    print(f"Total TTC: {data['montants'].get('total_ttc', 0):.2f} €")
    print(f"Surface totale: {data['montants'].get('surface_totale', 0):.2f} m²")
    
    print(f"\n=== LIGNES: {len(data['lignes'])} ===")
    
    df = parser.to_dataframe()
    if not df.empty:
        print(f"\n📊 DataFrame généré: {df.shape[0]} lignes × {df.shape[1]} colonnes")
        print("\n✅ Parser fonctionne correctement !")
    else:
        print("\n⚠️  Aucune ligne extraite, vérifier le format du PDF")