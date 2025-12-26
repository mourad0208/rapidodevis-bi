"""
Test du parser sur plusieurs PDFs
Génère un rapport de compatibilité
"""

from pathlib import Path
from pdf_parser import RapidoDevisParser
import pandas as pd
from tqdm import tqdm

def test_parser_batch(pdf_dir: str, limit: int = 50):
    """
    Teste le parser sur plusieurs PDFs
    
    Args:
        pdf_dir: Dossier contenant les PDFs
        limit: Nombre maximum de PDFs à tester
    """
    
    pdf_path = Path(pdf_dir)
    pdfs = list(pdf_path.glob("*.pdf"))[:limit]
    
    print(f"📊 Test du parser sur {len(pdfs)} PDFs\n")
    print("="*80)
    
    results = []
    
    for pdf_file in tqdm(pdfs, desc="Parsing PDFs"):
        result = {
            'fichier': pdf_file.name,
            'succes': False,
            'numero_devis': None,
            'client_nom': None,
            'client_ville': None,
            'nb_pieces': 0,
            'nb_lignes': 0,
            'montant_ht': 0,
            'montant_ttc': 0,
            'erreur': None
        }
        
        try:
            parser = RapidoDevisParser(str(pdf_file))
            data = parser.parse()
            
            result['succes'] = True
            result['numero_devis'] = data['metadata'].get('numero_devis')
            result['client_nom'] = data['client'].get('nom')
            result['client_ville'] = data['client'].get('ville')
            result['nb_pieces'] = len(data['pieces'])
            result['nb_lignes'] = len(data['lignes'])
            result['montant_ht'] = data['montants'].get('total_ht', 0)
            result['montant_ttc'] = data['montants'].get('total_ttc', 0)
            
        except Exception as e:
            result['erreur'] = str(e)
        
        results.append(result)
    
    # Créer DataFrame des résultats
    df = pd.DataFrame(results)
    
    # Statistiques
    print("\n" + "="*80)
    print("📈 STATISTIQUES")
    print("="*80)
    
    nb_succes = df['succes'].sum()
    nb_echecs = len(df) - nb_succes
    
    print(f"\n✅ Succès : {nb_succes}/{len(df)} ({nb_succes/len(df)*100:.1f}%)")
    print(f"❌ Échecs : {nb_echecs}/{len(df)} ({nb_echecs/len(df)*100:.1f}%)")
    
    print("\n--- Extraction des données ---")
    print(f"Numéro devis détecté    : {df['numero_devis'].notna().sum()}/{len(df)} ({df['numero_devis'].notna().sum()/len(df)*100:.1f}%)")
    print(f"Client nom détecté      : {df['client_nom'].notna().sum()}/{len(df)} ({df['client_nom'].notna().sum()/len(df)*100:.1f}%)")
    print(f"Client ville détectée   : {df['client_ville'].notna().sum()}/{len(df)} ({df['client_ville'].notna().sum()/len(df)*100:.1f}%)")
    print(f"Pièces extraites        : {df['nb_pieces'].sum()} au total (moy: {df['nb_pieces'].mean():.1f}/devis)")
    print(f"Lignes extraites        : {df['nb_lignes'].sum()} au total (moy: {df['nb_lignes'].mean():.1f}/devis)")
    
    print("\n--- Montants ---")
    print(f"Montant HT total        : {df['montant_ht'].sum():,.2f} €")
    print(f"Montant TTC total       : {df['montant_ttc'].sum():,.2f} €")
    print(f"Montant moyen HT        : {df['montant_ht'].mean():,.2f} €")
    
    # PDFs sans lignes extraites
    sans_lignes = df[df['nb_lignes'] == 0]
    if len(sans_lignes) > 0:
        print(f"\n⚠️  {len(sans_lignes)} PDFs sans lignes extraites")
        print("\nExemples (5 premiers) :")
        for idx, row in sans_lignes.head(5).iterrows():
            print(f"  - {row['fichier']}")
    
    # Erreurs
    avec_erreurs = df[df['erreur'].notna()]
    if len(avec_erreurs) > 0:
        print(f"\n❌ {len(avec_erreurs)} PDFs avec erreurs")
        print("\nExemples d'erreurs :")
        for erreur in avec_erreurs['erreur'].unique()[:3]:
            print(f"  - {erreur}")
    
    # Sauvegarder le rapport
    report_path = Path("logs/parser_test_report.csv")
    report_path.parent.mkdir(exist_ok=True)
    df.to_csv(report_path, index=False)
    print(f"\n💾 Rapport détaillé sauvegardé : {report_path}")
    
    return df


if __name__ == "__main__":
    import sys
    
    pdf_dir = "data/pdfs"
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    
    df_results = test_parser_batch(pdf_dir, limit=limit)
    
    print("\n✅ Test terminé !")
    print(f"\nPour tester plus de PDFs : python scripts/test_parser_batch.py 100")