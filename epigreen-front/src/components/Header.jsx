import React, { useState } from 'react';
import { Link } from 'react-router-dom';

export default function Header({ userName, onSearch }) {
    // État local pour stocker le texte de recherche
    const [searchInput, setSearchInput] = useState("");

    // Fonction de déconnexion (Bouton gris)
    const handleLogout = () => {
        localStorage.clear();
        window.location.href = '/login'; 
    };

    // Déclencher la recherche au clic du bouton
    const handleSearchClick = () => {
        if (onSearch) onSearch(searchInput);
    };

    // Déclencher la recherche avec la touche "Entrée"
    const handleKeyDown = (e) => {
        if (e.key === 'Enter') {
            handleSearchClick();
        }
    };

    return (
        <div className="topbar">
            {/* Logo et retour à l'accueil */}
            <Link to="/" className="brand">🌿 Epigreen</Link>

            {/* Zone de recherche au centre avec bouton */}
            <div style={{ flex: 1, display: 'flex', justifyContent: 'center', gap: '5px' }}>
                <input 
                    type="text" 
                    placeholder="Rechercher..." 
                    style={{ width: '100%', maxWidth: '300px' }}
                    value={searchInput}
                    onChange={(e) => setSearchInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                />
                <button onClick={handleSearchClick} style={{ padding: '10px 15px' }}>
                     Rechercher
                </button>
            </div>

            {/* Salutation, Panier et Déconnexion à droite */}
            <div className="row" style={{ marginLeft: 'auto', gap: '20px' }}>
                <span style={{ fontWeight: 'bold' }}>
                    Bonjour {userName || 'Invité'} !
                </span>
                
                {/* Icône du panier d'achat */}
                <Link to="/cart" style={{ fontSize: '24px', position: 'relative', textDecoration: 'none' }}>
                    🛒 
                    <span className="badge" style={{ 
                        position: 'absolute', top: '-5px', right: '-10px', 
                        background: 'var(--danger)', color: 'white', border: 'none', fontSize: '11px'
                    }}>0</span>
                </Link>

                {/* Bouton gris de déconnexion */}
                <button className="small" onClick={handleLogout} style={{ background: '#666' }}>
                    Déconnexion
                </button>
            </div>
        </div>
    );
}