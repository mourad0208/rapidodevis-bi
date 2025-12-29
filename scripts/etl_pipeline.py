"""
RAPIDO'DEVIS - ETL PIPELINE PRINCIPAL
Orchestration complète : PDFs + WordPress → PostgreSQL
"""

import os
import sys
from pathlib import Path
from typing import List, Dict
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

# Import des modules locaux
from pdf_parser import RapidoDevisParser
from wordpress_connector import WordPressConnector

# Charger les variables d'environnement
load_dotenv()


class RapidoDevisETL:
    """Pipeline ETL complet pour Rapido'Devis"""
    
    def __init__(self, db_config: Dict):
        """
        Args:
            db_config: Configuration PostgreSQL
        """
        self.db_config = db_config
        self.conn = None
        
    def connect_db(self):
        """Connexion à PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            print("✅ Connexion PostgreSQL réussie")
        except Exception as e:
            print(f"❌ Erreur connexion DB: {e}")
            raise
    
    def close_db(self):
        """Fermeture connexion"""
        if self.conn:
            self.conn.close()
            print("📊 Connexion DB fermée")
    
    # ============================================
    # ÉTAPE 1: EXTRACTION
    # ============================================
    
    def extract_pdfs(self, pdf_directory: str, limit: int = None) -> Dict[str, pd.DataFrame]:
        """
        Extrait tous les PDFs d'un répertoire
        
        Args:
            pdf_directory: Chemin vers le dossier contenant les PDFs
            limit: Limite le nombre de PDFs à traiter (None = tous)
            
        Returns:
            Dict de DataFrames (devis, pieces, lignes)
        """
        print(f"\n📄 EXTRACTION DES PDFs depuis {pdf_directory}")
        
        pdf_path = Path(pdf_directory)
        pdf_files = list(pdf_path.glob("*.pdf"))
        
        if limit:
            pdf_files = pdf_files[:limit]
        
        print(f"   {len(pdf_files)} fichiers PDF à traiter")
        
        all_devis = []
        all_lignes = []
        all_pieces = []
        errors = []
        
        for pdf_file in tqdm(pdf_files, desc="Parsing PDFs"):
            try:
                parser = RapidoDevisParser(str(pdf_file))
                data = parser.parse()
                
                # Construire l'enregistrement devis
                devis = {
                    **data['metadata'],
                    **data['client'],
                    **data['montants'],
                    'adresse_chantier': data['chantier'].get('adresse'),
                    'code_postal_chantier': data['chantier'].get('code_postal'),
                    'ville_chantier': data['chantier'].get('ville'),
                    'pdf_path': str(pdf_file),
                    'pdf_hash': data['pdf_info']['hash']
                }
                all_devis.append(devis)
                
                # Pièces
                for piece in data['pieces']:
                    piece['numero_devis'] = devis.get('numero_devis')
                    all_pieces.append(piece)
                
                # Lignes
                for ligne in data['lignes']:
                    ligne['numero_devis'] = devis.get('numero_devis')
                    all_lignes.append(ligne)
                
            except Exception as e:
                errors.append({'fichier': pdf_file.name, 'erreur': str(e)})
                continue
        
        print(f"\n✅ Extraction terminée: {len(all_devis)} devis, {len(all_lignes)} lignes")
        if errors:
            print(f"⚠️  {len(errors)} erreurs")
        
        return {
            'devis': pd.DataFrame(all_devis),
            'pieces': pd.DataFrame(all_pieces),
            'lignes': pd.DataFrame(all_lignes),
            'errors': pd.DataFrame(errors)
        }
    
    def extract_wordpress(self, wp_url: str, username: str = None, password: str = None) -> Dict[str, pd.DataFrame]:
        """
        Extrait les données de WordPress
        
        Args:
            wp_url: URL du site WordPress
            username: Username WordPress (optionnel)
            password: Application Password (optionnel)
            
        Returns:
            Dict de DataFrames (clients, paiements)
        """
        print(f"\n🌐 EXTRACTION WORDPRESS depuis {wp_url}")
        
        connector = WordPressConnector(wp_url, username, password)
        dfs = connector.to_dataframes()
        
        print(f"✅ Clients WP: {len(dfs.get('clients', []))}")
        print(f"✅ Paiements WP: {len(dfs.get('paiements', []))}")
        
        return dfs
    
    # ============================================
    # ÉTAPE 2: TRANSFORMATION
    # ============================================
    
    def transform_data(self, pdf_data: Dict, wp_data: Dict) -> Dict:
        """
        Nettoie et transforme les données
        
        Args:
            pdf_data: Données extraites des PDFs
            wp_data: Données extraites de WordPress
            
        Returns:
            Données transformées prêtes pour chargement
        """
        print("\n🔄 TRANSFORMATION DES DONNÉES")
        
        transformed = {}
        
        # 1. CLIENTS : fusion PDF + WordPress (seulement clients actifs)
        clients_pdf = pdf_data['devis'][['nom', 'adresse', 'code_postal', 'ville', 'type_client']].copy()
        clients_pdf = clients_pdf.dropna(subset=['nom'])  # Seulement ceux avec nom
        clients_pdf = clients_pdf.drop_duplicates(subset=['nom'], keep='first')
        
        clients_wp = wp_data.get('clients', pd.DataFrame())
        
        if not clients_wp.empty:
            # FILTRE : Seulement les clients WooCommerce avec au moins 1 commande
            clients_wp = clients_wp[clients_wp['nombre_commandes'] > 0].copy()
            print(f"   (Filtré: {len(clients_wp)} clients WooCommerce actifs)")
            
            # Combiner PDF + WP
            transformed['clients'] = pd.concat([clients_pdf, clients_wp], ignore_index=True)
            transformed['clients'] = transformed['clients'].drop_duplicates(subset=['nom'], keep='last')
        else:
            transformed['clients'] = clients_pdf

        # 2. DEVIS
        transformed['devis'] = pdf_data['devis'].copy()
        transformed['devis']['statut'] = 'EN ATTENTE'
        transformed['devis']['passoire_thermique'] = False
        
        # 3. PIÈCES
        transformed['pieces'] = pdf_data['pieces'].copy()
        
        # 4. LIGNES DEVIS
        transformed['lignes'] = pdf_data['lignes'].copy()
        
        # 5. PAIEMENTS
        transformed['paiements'] = wp_data.get('paiements', pd.DataFrame())
        
        print(f"✅ Transformation terminée")
        print(f"   - Clients: {len(transformed['clients'])}")
        print(f"   - Devis: {len(transformed['devis'])}")
        print(f"   - Lignes: {len(transformed['lignes'])}")
        print(f"   - Paiements: {len(transformed.get('paiements', []))}")
        
        return transformed
    
    # ============================================
    # ÉTAPE 3: CHARGEMENT
    # ============================================
    
    def load_to_postgres(self, data: Dict):
        """
        Charge les données dans PostgreSQL
        
        Args:
            data: Dictionnaire de DataFrames transformés
        """
        print("\n💾 CHARGEMENT DANS POSTGRESQL")
        
        cursor = self.conn.cursor()
        
        try:
            # 1. CLIENTS
            print("   Insertion clients...", end='')
            if not data['clients'].empty:
                self._upsert_clients(cursor, data['clients'])
            print(" ✓")
            
            # 2. DEVIS
            print("   Insertion devis...", end='')
            if not data['devis'].empty:
                self._insert_devis(cursor, data['devis'])
            print(" ✓")
            
            # 3. PIÈCES
            print("   Insertion pièces...", end='')
            if not data['pieces'].empty:
                self._insert_pieces(cursor, data['pieces'])
            print(" ✓")
            
            # 4. LIGNES DEVIS
            print("   Insertion lignes devis...", end='')
            if not data['lignes'].empty:
                self._insert_lignes(cursor, data['lignes'])
            print(" ✓")
            
            # 5. PAIEMENTS
            if 'paiements' in data and not data['paiements'].empty:
                print("   Insertion paiements...", end='')
                self._insert_paiements(cursor, data['paiements'])
                print(" ✓")
            
            # COMMIT
            self.conn.commit()
            print("\n✅ Chargement terminé avec succès")
            
        except Exception as e:
            self.conn.rollback()
            print(f"\n❌ Erreur lors du chargement: {e}")
            raise
        finally:
            cursor.close()
    
    def _upsert_clients(self, cursor, df: pd.DataFrame):
        """Insert ou update clients"""
        
        for idx, row in df.iterrows():
            # Debug
            wp_id = row.get('wordpress_id')
            if pd.notna(wp_id):
                try:
                    wp_id = int(wp_id)
                    if wp_id > 9223372036854775807 or wp_id < -9223372036854775808:
                        print(f"⚠️  wordpress_id hors limite BIGINT: {wp_id} pour {row.get('nom')}")
                        wp_id = None
                except (ValueError, OverflowError) as e:
                    print(f"⚠️  Erreur conversion wordpress_id: {wp_id} ({type(wp_id)}) pour {row.get('nom')}")
                    wp_id = None
            else:
                wp_id = None
            
            try:
                cursor.execute("""
                    INSERT INTO clients (nom, prenom, adresse, code_postal, ville, type_client, wordpress_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nom) 
                    DO UPDATE SET
                        adresse = EXCLUDED.adresse,
                        ville = EXCLUDED.ville,
                        wordpress_sync_at = CURRENT_TIMESTAMP
                    RETURNING id
                """, (
                    row.get('nom'),
                    row.get('prenom'),
                    row.get('adresse'),
                    row.get('code_postal'),
                    row.get('ville'),
                    row.get('type_client', 'PARTICULIER'),
                    wp_id
                ))
            except Exception as e:
                print(f"❌ Erreur insertion client: {row.get('nom')}")
                print(f"   wordpress_id: {wp_id} (type: {type(wp_id)})")
                print(f"   Erreur: {e}")
                raise
    
    def _insert_devis(self, cursor, df: pd.DataFrame):
        """Insert devis (skip si doublon)"""
        
        for _, row in df.iterrows():
            # Récupérer client_id
            if pd.notna(row.get('nom')):
                cursor.execute("SELECT id FROM clients WHERE nom = %s", (row.get('nom'),))
                result = cursor.fetchone()
                client_id = result[0] if result else None
            else:
                client_id = None
            
            # Vérifier doublon
            cursor.execute("SELECT id FROM devis WHERE numero_devis = %s", (row.get('numero_devis'),))
            if cursor.fetchone():
                continue
            
            # Nettoyer les dates (remplacer NaN par None)
            date_validite = row.get('date_validite')
            if pd.isna(date_validite):
                date_validite = None
            
            # Nettoyer les montants (remplacer NaN par 0)
            total_ht = row.get('total_ht', 0)
            if pd.isna(total_ht):
                total_ht = 0
            
            total_ttc = row.get('total_ttc', 0)
            if pd.isna(total_ttc):
                total_ttc = 0
            
            tva_10 = row.get('tva_10_pct', 0)
            if pd.isna(tva_10):
                tva_10 = 0
                
            tva_20 = row.get('tva_20_pct', 0)
            if pd.isna(tva_20):
                tva_20 = 0
            
            surface_totale = row.get('surface_totale')
            if pd.isna(surface_totale):
                surface_totale = None
            
            cursor.execute("""
                INSERT INTO devis (
                    numero_devis, client_id, adresse_chantier, code_postal_chantier, 
                    ville_chantier, surface_totale, date_devis, date_validite,
                    statut, total_ht, tva_10_pct, tva_20_pct, total_ttc,
                    passoire_thermique, pdf_path, pdf_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get('numero_devis'),
                client_id,
                row.get('adresse_chantier'),
                row.get('code_postal_chantier'),
                row.get('ville_chantier'),
                surface_totale,
                row.get('date_devis'),
                date_validite,  # Nettoyée
                row.get('statut', 'EN ATTENTE'),
                total_ht,  # Nettoyé
                tva_10,    # Nettoyé
                tva_20,    # Nettoyé
                total_ttc, # Nettoyé
                row.get('passoire_thermique', False),
                row.get('pdf_path'),
                row.get('pdf_hash')
            ))
    
    def _insert_pieces(self, cursor, df: pd.DataFrame):
        """Insert pièces travaux"""
        
        for _, row in df.iterrows():
            cursor.execute("SELECT id FROM devis WHERE numero_devis = %s", (row.get('numero_devis'),))
            result = cursor.fetchone()
            if not result:
                continue
            
            devis_id = result[0]
            
            cursor.execute("""
                INSERT INTO pieces_travaux (devis_id, nom_piece, surface)
                VALUES (%s, %s, %s)
            """, (devis_id, row.get('nom'), row.get('surface')))
    
    def _insert_lignes(self, cursor, df: pd.DataFrame):
        """Insert lignes de devis"""
        
        for _, row in df.iterrows():
            cursor.execute("SELECT id FROM devis WHERE numero_devis = %s", (row.get('numero_devis'),))
            result = cursor.fetchone()
            if not result:
                continue
            
            devis_id = result[0]
            
            # Récupérer piece_id (optionnel)
            piece_id = None
            if pd.notna(row.get('piece')):
                cursor.execute("""
                    SELECT id FROM pieces_travaux 
                    WHERE devis_id = %s AND nom_piece = %s 
                    LIMIT 1
                """, (devis_id, row.get('piece')))
                piece_result = cursor.fetchone()
                if piece_result:
                    piece_id = piece_result[0]
            
            cursor.execute("""
                INSERT INTO lignes_devis (
                    devis_id, piece_id, numero_ligne, designation, description,
                    categorie, quantite, unite, prix_unitaire_ht, taux_tva, total_ht
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                devis_id, piece_id, row.get('numero_ligne'), 
                row.get('designation'), row.get('description'),
                row.get('categorie'), row.get('quantite'), row.get('unite'),
                row.get('prix_unitaire_ht'), row.get('taux_tva'), row.get('total_ht')
            ))
    
    def _insert_paiements(self, cursor, df: pd.DataFrame):
        """Insert paiements"""
        
        for _, row in df.iterrows():
            # Récupérer client_id par email
            if pd.notna(row.get('client_email')):
                cursor.execute("SELECT id FROM clients WHERE email = %s", (row.get('client_email'),))
                result = cursor.fetchone()
                client_id = result[0] if result else None
            else:
                client_id = None
            
            cursor.execute("""
                INSERT INTO paiements (
                    client_id, montant, methode_paiement, statut_paiement,
                    date_paiement, reference_transaction, wordpress_order_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                client_id,
                row.get('montant'),
                row.get('methode_paiement'),
                row.get('statut_paiement'),
                row.get('date_paiement'),
                row.get('reference_transaction'),
                row.get('wordpress_order_id')
            ))
    
    # ============================================
    # PIPELINE COMPLET
    # ============================================
    
    def run_full_pipeline(self, 
                         pdf_directory: str,
                         pdf_limit: int = None,
                         wordpress_url: str = None,
                         wp_username: str = None,
                         wp_password: str = None):
        """
        Exécute le pipeline ETL complet
        
        Args:
            pdf_directory: Dossier contenant les PDFs
            pdf_limit: Limite de PDFs à traiter (None = tous)
            wordpress_url: URL WordPress (optionnel)
            wp_username: Username WP (optionnel)
            wp_password: Password WP (optionnel)
        """
        
        print("\n" + "="*60)
        print(" RAPIDO'DEVIS - PIPELINE ETL COMPLET")
        print("="*60)
        
        try:
            # Connexion DB
            self.connect_db()
            
            # EXTRACTION
            pdf_data = self.extract_pdfs(pdf_directory, limit=pdf_limit)
            
            if wordpress_url:
                wp_data = self.extract_wordpress(wordpress_url, wp_username, wp_password)
            else:
                wp_data = {'clients': pd.DataFrame(), 'paiements': pd.DataFrame()}
            
            # TRANSFORMATION
            transformed_data = self.transform_data(pdf_data, wp_data)
            
            # CHARGEMENT
            self.load_to_postgres(transformed_data)
            
            print("\n" + "="*60)
            print("✅ PIPELINE TERMINÉ AVEC SUCCÈS !")
            print("="*60)
            
        except Exception as e:
            print(f"\n❌ ERREUR PIPELINE: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            self.close_db()


# ============================================
# SCRIPT PRINCIPAL
# ============================================

if __name__ == "__main__":
    # Configuration PostgreSQL
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'rapidodevis'),
        'user': os.getenv('DB_USER', 'rapido'),
        'password': os.getenv('DB_PASSWORD', 'password')
    }
    
    # Configuration sources
    PDF_DIRECTORY = "data/pdfs"
    PDF_LIMIT = int(os.getenv('PDF_LIMIT', 0)) or None  # 0 = tous
    
    WORDPRESS_URL = os.getenv('WORDPRESS_URL')
    WORDPRESS_USERNAME = os.getenv('WORDPRESS_USERNAME')
    WORDPRESS_PASSWORD = os.getenv('WORDPRESS_APP_PASSWORD')
    
    # Créer et lancer le pipeline
    etl = RapidoDevisETL(DB_CONFIG)
    
    etl.run_full_pipeline(
        PDF_DIRECTORY,
        pdf_limit=PDF_LIMIT,
        wordpress_url=WORDPRESS_URL,
        wp_username=WORDPRESS_USERNAME,
        wp_password=WORDPRESS_PASSWORD
    )