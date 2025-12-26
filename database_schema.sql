-- ============================================
-- RAPIDO'DEVIS - SCHÉMA BASE DE DONNÉES
-- PostgreSQL 15+
-- ============================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- TABLE: clients

CREATE TABLE clients (
    id SERIAL PRIMARY KEY,
    uuid UUID DEFAULT uuid_generate_v4() UNIQUE,
    nom VARCHAR(255) NOT NULL,
    prenom VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    telephone VARCHAR(50),
    adresse TEXT,
    code_postal VARCHAR(10),
    ville VARCHAR(100),
    type_client VARCHAR(50) NOT NULL CHECK (type_client IN ('PRO IMMOBILIER', 'PRO BÂTIMENT', 'PARTICULIER')),
    
    -- Métriques client
    nombre_devis INTEGER DEFAULT 0,
    ca_total_ht DECIMAL(12,2) DEFAULT 0.00,
    ca_total_ttc DECIMAL(12,2) DEFAULT 0.00,
    taux_conversion DECIMAL(5,2) DEFAULT 0.00,
    
    -- Provenance WordPress
    wordpress_id INTEGER UNIQUE,
    wordpress_sync_at TIMESTAMP,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE INDEX idx_clients_type ON clients(type_client);
CREATE INDEX idx_clients_wordpress_id ON clients(wordpress_id);
CREATE INDEX idx_clients_email ON clients(email);

-- ============================================
-- TABLE: devis
-- ============================================
CREATE TABLE devis (
    id SERIAL PRIMARY KEY,
    numero_devis VARCHAR(50) UNIQUE NOT NULL,
    client_id INTEGER REFERENCES clients(id) ON DELETE SET NULL,
    
    -- Informations chantier
    adresse_chantier TEXT,
    code_postal_chantier VARCHAR(10),
    ville_chantier VARCHAR(100),
    surface_totale DECIMAL(10,2),
    
    -- Dates
    date_devis DATE NOT NULL,
    date_validite DATE,
    date_acceptation DATE,
    date_refus DATE,
    
    -- Statut
    statut VARCHAR(50) NOT NULL DEFAULT 'EN ATTENTE' CHECK (
        statut IN ('EN ATTENTE', 'ACCEPTÉ', 'REFUSÉ', 'EXPIRÉ', 'EN COURS', 'TERMINÉ')
    ),
    
    -- Montants
    total_ht DECIMAL(12,2) NOT NULL,
    tva_10_pct DECIMAL(12,2) DEFAULT 0.00,
    tva_20_pct DECIMAL(12,2) DEFAULT 0.00,
    total_ttc DECIMAL(12,2) NOT NULL,
    
    -- Classification DPE
    classe_dpe_avant VARCHAR(1) CHECK (classe_dpe_avant IN ('A','B','C','D','E','F','G')),
    classe_dpe_apres VARCHAR(1) CHECK (classe_dpe_apres IN ('A','B','C','D','E','F','G')),
    passoire_thermique BOOLEAN DEFAULT FALSE,
    
    -- Fichier source
    pdf_path TEXT,
    pdf_hash VARCHAR(64),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE INDEX idx_devis_numero ON devis(numero_devis);
CREATE INDEX idx_devis_client ON devis(client_id);
CREATE INDEX idx_devis_statut ON devis(statut);
CREATE INDEX idx_devis_date ON devis(date_devis DESC);

-- ============================================
-- TABLE: pieces_travaux
-- ============================================
CREATE TABLE pieces_travaux (
    id SERIAL PRIMARY KEY,
    devis_id INTEGER REFERENCES devis(id) ON DELETE CASCADE,
    
    nom_piece VARCHAR(100) NOT NULL,
    surface DECIMAL(10,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pieces_devis ON pieces_travaux(devis_id);

-- ============================================
-- TABLE: lignes_devis
-- ============================================
CREATE TABLE lignes_devis (
    id SERIAL PRIMARY KEY,
    devis_id INTEGER REFERENCES devis(id) ON DELETE CASCADE,
    piece_id INTEGER REFERENCES pieces_travaux(id) ON DELETE SET NULL,
    
    numero_ligne VARCHAR(20),
    designation TEXT NOT NULL,
    description TEXT,
    
    -- Catégorisation
    categorie VARCHAR(100),
    sous_categorie VARCHAR(100),
    
    -- Quantités et prix
    quantite DECIMAL(10,2) NOT NULL,
    unite VARCHAR(20) NOT NULL,
    prix_unitaire_ht DECIMAL(10,2) NOT NULL,
    taux_tva DECIMAL(5,2) NOT NULL,
    total_ht DECIMAL(12,2) NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_lignes_devis ON lignes_devis(devis_id);
CREATE INDEX idx_lignes_categorie ON lignes_devis(categorie);

-- ============================================
-- TABLE: paiements
-- ============================================
CREATE TABLE paiements (
    id SERIAL PRIMARY KEY,
    devis_id INTEGER REFERENCES devis(id) ON DELETE CASCADE,
    client_id INTEGER REFERENCES clients(id) ON DELETE SET NULL,
    
    montant DECIMAL(12,2) NOT NULL,
    methode_paiement VARCHAR(50) CHECK (
        methode_paiement IN ('CB', 'VIREMENT', 'CHEQUE', 'ESPECES', 'STRIPE', 'PAYPAL')
    ),
    
    statut_paiement VARCHAR(50) DEFAULT 'EN ATTENTE' CHECK (
        statut_paiement IN ('EN ATTENTE', 'VALIDÉ', 'ÉCHOUÉ', 'REMBOURSÉ')
    ),
    
    date_paiement DATE NOT NULL,
    reference_transaction VARCHAR(255),
    
    -- Provenance WordPress
    wordpress_order_id INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_paiements_devis ON paiements(devis_id);
CREATE INDEX idx_paiements_client ON paiements(client_id);
CREATE INDEX idx_paiements_date ON paiements(date_paiement DESC);
CREATE INDEX idx_paiements_statut ON paiements(statut_paiement);

-- ============================================
-- TABLE: statistiques_travaux
-- ============================================
CREATE TABLE statistiques_travaux (
    id SERIAL PRIMARY KEY,
    
    type_travaux VARCHAR(100) NOT NULL,
    
    -- Agrégations
    nombre_demandes INTEGER DEFAULT 0,
    montant_moyen DECIMAL(12,2),
    montant_total DECIMAL(12,2),
    pourcentage_total DECIMAL(5,2),
    
    -- Période
    periode_type VARCHAR(20) CHECK (periode_type IN ('JOUR', 'SEMAINE', 'MOIS', 'ANNÉE')),
    periode_date DATE NOT NULL,
    
    -- Tendance
    tendance VARCHAR(20) CHECK (tendance IN ('HAUSSE', 'BAISSE', 'STABLE')),
    evolution_pct DECIMAL(5,2),
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(type_travaux, periode_type, periode_date)
);

CREATE INDEX idx_stats_travaux_type ON statistiques_travaux(type_travaux);
CREATE INDEX idx_stats_travaux_periode ON statistiques_travaux(periode_date DESC);

-- ============================================
-- TABLE: statistiques_regions
-- ============================================
CREATE TABLE statistiques_regions (
    id SERIAL PRIMARY KEY,
    
    region VARCHAR(100) NOT NULL,
    departement VARCHAR(100),
    
    -- Métriques
    nombre_estimations INTEGER DEFAULT 0,
    montant_total DECIMAL(12,2),
    nombre_passoires_thermiques INTEGER DEFAULT 0,
    pourcentage_passoires DECIMAL(5,2),
    
    -- Période
    periode_type VARCHAR(20) CHECK (periode_type IN ('MOIS', 'TRIMESTRE', 'ANNÉE')),
    periode_date DATE NOT NULL,
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(region, periode_type, periode_date)
);

CREATE INDEX idx_stats_regions_region ON statistiques_regions(region);
CREATE INDEX idx_stats_regions_periode ON statistiques_regions(periode_date DESC);

-- ============================================
-- VUES MATÉRIALISÉES
-- ============================================

-- Vue: KPIs globaux
CREATE MATERIALIZED VIEW mv_kpis_globaux AS
SELECT 
    COUNT(DISTINCT d.id) AS total_estimations,
    SUM(d.total_ht) AS montant_total_ht,
    SUM(d.total_ttc) AS montant_total_ttc,
    COUNT(DISTINCT d.client_id) AS clients_actifs,
    COUNT(CASE WHEN d.passoire_thermique THEN 1 END) AS passoires_thermiques,
    ROUND(
        COUNT(CASE WHEN d.passoire_thermique THEN 1 END)::DECIMAL / 
        NULLIF(COUNT(d.id), 0) * 100, 
        1
    ) AS pct_passoires_thermiques
FROM devis d
WHERE d.deleted_at IS NULL;

-- Vue: Répartition clients par segment
CREATE MATERIALIZED VIEW mv_clients_segments AS
SELECT 
    type_client,
    COUNT(*) AS nombre_clients,
    ROUND(COUNT(*)::DECIMAL / SUM(COUNT(*)) OVER () * 100, 1) AS pourcentage,
    SUM(ca_total_ht) AS ca_total_ht
FROM clients
WHERE deleted_at IS NULL
GROUP BY type_client;

-- Vue: Top travaux demandés
CREATE MATERIALIZED VIEW mv_top_travaux AS
SELECT 
    l.categorie AS type_travaux,
    COUNT(*) AS nombre_demandes,
    AVG(l.prix_unitaire_ht * l.quantite) AS montant_moyen,
    SUM(l.total_ht) AS montant_total,
    ROUND(
        SUM(l.total_ht) / NULLIF(SUM(SUM(l.total_ht)) OVER (), 0) * 100,
        1
    ) AS pct_total
FROM lignes_devis l
JOIN devis d ON l.devis_id = d.id
WHERE d.deleted_at IS NULL
GROUP BY l.categorie
ORDER BY nombre_demandes DESC;

-- ============================================
-- FONCTIONS
-- ============================================

-- Fonction: Calculer taux de conversion client
CREATE OR REPLACE FUNCTION calculate_client_conversion_rate(p_client_id INTEGER)
RETURNS DECIMAL AS $$
DECLARE
    v_total_devis INTEGER;
    v_devis_acceptes INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_total_devis 
    FROM devis 
    WHERE client_id = p_client_id AND deleted_at IS NULL;
    
    SELECT COUNT(*) INTO v_devis_acceptes 
    FROM devis 
    WHERE client_id = p_client_id AND statut = 'ACCEPTÉ' AND deleted_at IS NULL;
    
    IF v_total_devis = 0 THEN
        RETURN 0.00;
    END IF;
    
    RETURN ROUND((v_devis_acceptes::DECIMAL / v_total_devis) * 100, 2);
END;
$$ LANGUAGE plpgsql;

-- Fonction: Refresh toutes les vues matérialisées
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_kpis_globaux;
    REFRESH MATERIALIZED VIEW mv_clients_segments;
    REFRESH MATERIALIZED VIEW mv_top_travaux;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- TRIGGERS
-- ============================================

-- Trigger: Mise à jour automatique du timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_clients_updated_at BEFORE UPDATE ON clients
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_devis_updated_at BEFORE UPDATE ON devis
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_paiements_updated_at BEFORE UPDATE ON paiements
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- COMMENTAIRES
-- ============================================

COMMENT ON TABLE clients IS 'Table principale des clients Rapido''Devis';
COMMENT ON TABLE devis IS 'Estimations et devis générés';
COMMENT ON TABLE lignes_devis IS 'Détail ligne par ligne des devis';
COMMENT ON TABLE paiements IS 'Suivi des paiements clients';
