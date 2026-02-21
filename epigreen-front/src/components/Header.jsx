import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import axios from 'axios';
import { CONFIG } from '../api/config';

export default function Header({ userName, onSearch }) {
    // État local pour stocker le texte de recherche
    const [searchInput, setSearchInput] = useState("");
    const [cartItemCount, setCartItemCount] = useState(0);
    const userId = localStorage.getItem('epigreen_user_id');

    // Fonction de déconnexion 
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


    // calculer le total des articles
    const fetchCartCount = () => {
        if (!userId) return;
        
        axios.get(`${CONFIG.API.CART}/${userId}`)
            .then(res => {
                if (res.data && res.data.items) {
                    // On additionne les quantités de chaque ligne du panier
                    const totalCount = res.data.items.reduce((sum, item) => sum + item.quantity, 0);
                    setCartItemCount(totalCount);
                } else {
                    setCartItemCount(0);
                }
            })
            .catch(err => {
                console.error("Erreur lors de la mise à jour du badge :", err);
                setCartItemCount(0);
            });
    };

    useEffect(() => {
        fetchCartCount(); // Récupération initiale

        // On écoute le signal envoyé par les autres pages
        window.addEventListener('cartUpdated', fetchCartCount);

        // Nettoyage propre quand le composant est détruit
        return () => {
            window.removeEventListener('cartUpdated', fetchCartCount);
        };
    }, [userId]);

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

            {/* userName, Panier et Bouton déconnexion*/}
            <div className="row" style={{ marginLeft: 'auto', gap: '20px' }}>
                <span style={{ fontWeight: 'bold' }}>
                    Bonjour {userName || 'Invité'} !
                </span>
                
                {/* Icône du panier d'achat */}
                <Link to="/cart" style={{ fontSize: '24px', position: 'relative', textDecoration: 'none' }}>
                    🛒 
                    {cartItemCount > 0 && (
                    <span className="badge" style={{ 
                        position: 'absolute', top: '-5px', right: '-10px', 
                        background: 'red', color: 'white', border: 'none', fontSize: '11px',
                        padding: '2px 6px', borderRadius: '50%'
                    }}>
                        {cartItemCount}
                    </span>
                )}
                </Link>

                {/* Bouton gris de déconnexion */}
                <button className="small" onClick={handleLogout} style={{ background: '#666',color: 'white' }}>
                    Déconnexion
                </button>
            </div>
        </div>
    );
}