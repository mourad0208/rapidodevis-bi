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