"""
RAPIDO'DEVIS - WORDPRESS API CONNECTOR
Récupération des données clients, CA et paiements depuis WordPress
"""

import requests
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from urllib.parse import urljoin


class WordPressConnector:
    """Connecteur WordPress REST API"""
    
    def __init__(self, base_url: str, username: str = None, app_password: str = None):
        """
        Args:
            base_url: URL du site WordPress (ex: https://rapido-devis.fr)
            username: Nom d'utilisateur WordPress (optionnel)
            app_password: Mot de passe d'application WordPress (optionnel)
        """
        self.base_url = base_url.rstrip('/')
        self.api_base = f"{self.base_url}/wp-json/wp/v2"
        self.wc_api_base = f"{self.base_url}/wp-json/wc/v3"
        
        self.session = requests.Session()
        
        if username and app_password:
            self.session.auth = (username, app_password)
    
    def test_connection(self) -> Dict:
        """Test la connexion à l'API WordPress"""
        
        try:
            response = self.session.get(f"{self.api_base}")
            
            if response.status_code == 200:
                return {
                    'success': True,
                    'message': 'Connexion WordPress OK',
                    'data': response.json()
                }
            else:
                return {
                    'success': False,
                    'message': f'Erreur {response.status_code}',
                    'data': None
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Erreur connexion: {str(e)}',
                'data': None
            }
    
    def get_users(self, per_page: int = 100) -> List[Dict]:
        """
        Récupère tous les utilisateurs WordPress
        
        Returns:
            Liste de dictionnaires avec les infos utilisateurs
        """
        all_users = []
        page = 1
        
        while True:
            response = self.session.get(
                f"{self.api_base}/users",
                params={'per_page': per_page, 'page': page}
            )
            
            if response.status_code == 200:
                users = response.json()
                if not users:
                    break
                
                all_users.extend([
                    {
                        'wordpress_id': user['id'],
                        'nom': user.get('name', ''),
                        'email': user.get('email', ''),
                        'wordpress_sync_at': datetime.now()
                    }
                    for user in users
                ])
                page += 1
            else:
                print(f"Erreur récupération users page {page}: {response.status_code}")
                break
        
        return all_users
    
    def get_woocommerce_customers(self, per_page: int = 100) -> List[Dict]:
        """
        Récupère les clients WooCommerce (si installé)
        
        Returns:
            Liste des clients avec métadonnées complètes
        """
        all_customers = []
        page = 1
        
        while True:
            response = self.session.get(
                f"{self.wc_api_base}/customers",
                params={'per_page': per_page, 'page': page}
            )
            
            if response.status_code == 200:
                customers = response.json()
                if not customers:
                    break
                
                for customer in customers:
                    billing = customer.get('billing', {})
                    
                    all_customers.append({
                        'wordpress_id': customer['id'],
                        'nom': billing.get('last_name', customer.get('last_name', '')),
                        'prenom': billing.get('first_name', customer.get('first_name', '')),
                        'email': customer.get('email', ''),
                        'telephone': billing.get('phone', ''),
                        'adresse': billing.get('address_1', ''),
                        'code_postal': billing.get('postcode', ''),
                        'ville': billing.get('city', ''),
                        'ca_total_ttc': float(customer.get('total_spent', 0)),
                        'nombre_commandes': customer.get('orders_count', 0),
                        'wordpress_sync_at': datetime.now()
                    })
                
                page += 1
            else:
                print(f"Erreur récupération customers page {page}: {response.status_code}")
                break
        
        return all_customers
    
    def get_orders(self, 
                   status: str = 'any', 
                   after: str = None,
                   per_page: int = 100) -> List[Dict]:
        """
        Récupère les commandes WooCommerce (= paiements)
        
        Args:
            status: Statut des commandes ('completed', 'processing', 'any')
            after: Date ISO format (ex: '2024-01-01T00:00:00')
            per_page: Nombre de résultats par page
        
        Returns:
            Liste des commandes/paiements
        """
        all_orders = []
        page = 1
        
        params = {
            'per_page': per_page,
            'status': status,
            'orderby': 'date',
            'order': 'desc'
        }
        
        if after:
            params['after'] = after
        
        while True:
            params['page'] = page
            response = self.session.get(
                f"{self.wc_api_base}/orders",
                params=params
            )
            
            if response.status_code == 200:
                orders = response.json()
                if not orders:
                    break
                
                for order in orders:
                    all_orders.append({
                        'wordpress_order_id': order['id'],
                        'client_email': order.get('billing', {}).get('email'),
                        'montant': float(order.get('total', 0)),
                        'methode_paiement': self._map_payment_method(
                            order.get('payment_method', 'unknown')
                        ),
                        'statut_paiement': self._map_order_status(order.get('status')),
                        'date_paiement': datetime.fromisoformat(
                            order['date_created'].replace('Z', '+00:00')
                        ).date(),
                        'reference_transaction': order.get('transaction_id', ''),
                        'created_at': datetime.now()
                    })
                
                page += 1
            else:
                print(f"Erreur récupération orders page {page}: {response.status_code}")
                break
        
        return all_orders
    
    def _map_payment_method(self, wc_method: str) -> str:
        """Convertit les méthodes de paiement WooCommerce vers notre schéma"""
        
        mapping = {
            'stripe': 'STRIPE',
            'paypal': 'PAYPAL',
            'bacs': 'VIREMENT',
            'cheque': 'CHEQUE',
            'cod': 'ESPECES',
            'unknown': 'CB'
        }
        
        return mapping.get(wc_method, 'CB')
    
    def _map_order_status(self, wc_status: str) -> str:
        """Convertit les statuts de commande WooCommerce"""
        
        mapping = {
            'completed': 'VALIDÉ',
            'processing': 'EN ATTENTE',
            'on-hold': 'EN ATTENTE',
            'pending': 'EN ATTENTE',
            'failed': 'ÉCHOUÉ',
            'cancelled': 'ÉCHOUÉ',
            'refunded': 'REMBOURSÉ'
        }
        
        return mapping.get(wc_status, 'EN ATTENTE')
    
    def get_statistics(self) -> Dict:
        """Récupère des statistiques globales"""
        
        stats = {}
        
        try:
            response = self.session.get(f"{self.wc_api_base}/reports/sales")
            if response.status_code == 200:
                sales_data = response.json()
                
                # L'API peut retourner une liste ou un dict
                if isinstance(sales_data, list) and len(sales_data) > 0:
                    sales_data = sales_data[0]
                
                if isinstance(sales_data, dict):
                    stats['ca_total'] = float(sales_data.get('total_sales', 0))
                    stats['nb_commandes'] = int(sales_data.get('total_orders', 0))
        except Exception as e:
            print(f"⚠️  Erreur récupération stats: {e}")
        
        return stats
    
    def to_dataframes(self) -> Dict[str, pd.DataFrame]:
        """
        Récupère toutes les données et les retourne en DataFrames
        
        Returns:
            Dict avec 'clients', 'paiements', 'stats'
        """
        
        print("Récupération des clients WooCommerce...")
        customers = self.get_woocommerce_customers()
        
        print("Récupération des commandes...")
        orders = self.get_orders(status='any')
        
        print("Récupération des statistiques...")
        stats = self.get_statistics()
        
        return {
            'clients': pd.DataFrame(customers) if customers else pd.DataFrame(),
            'paiements': pd.DataFrame(orders) if orders else pd.DataFrame(),
            'stats': pd.DataFrame([stats]) if stats else pd.DataFrame()
        }


# ============================================
# SCRIPT DE TEST
# ============================================

if __name__ == "__main__":
    import sys
    
    # Configuration
    WORDPRESS_URL = "https://rapido-devis.fr"
    
    print("="*60)
    print("TEST CONNEXION WORDPRESS")
    print("="*60)
    print(f"\nURL: {WORDPRESS_URL}")
    print("Tentative de connexion...\n")
    
    try:
        # Pour tester sans auth
        connector = WordPressConnector(WORDPRESS_URL)
        
        result = connector.test_connection()
        
        if result['success']:
            print("✅ Connexion réussie !")
            print(f"\nAPI disponible: {WORDPRESS_URL}/wp-json/")
            
            # Test récupération users (public)
            print("\n" + "="*60)
            print("TEST RÉCUPÉRATION USERS")
            print("="*60)
            
            try:
                users = connector.get_users()
                print(f"\n✅ {len(users)} utilisateurs récupérés")
                if users:
                    print("\nExemple (premier user):")
                    print(f"  - Nom: {users[0].get('nom')}")
                    print(f"  - Email: {users[0].get('email')}")
            except Exception as e:
                print(f"\n⚠️  Erreur: {e}")
                print("\nNote: La récupération des users nécessite une authentification.")
        
        else:
            print(f"❌ Échec de connexion: {result['message']}")
        
    except Exception as e:
        print(f"❌ ERREUR CRITIQUE: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("POUR UTILISER AVEC AUTHENTIFICATION:")
    print("="*60)
    print("""
1. Créer un Application Password dans WordPress:
   - WordPress Admin > Users > Your Profile
   - Section "Application Passwords"
   - Nom: "Rapido BI ETL"
   - Copier le mot de passe généré

2. Utiliser dans le code:
   connector = WordPressConnector(
       WORDPRESS_URL,
       username="votre_username",
       app_password="xxxx xxxx xxxx xxxx"
   )
   
3. Récupérer les données:
   dfs = connector.to_dataframes()
   print(f"Clients: {len(dfs['clients'])}")
   print(f"Paiements: {len(dfs['paiements'])}")
    """)
