import React, { useState, useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import axios from 'axios';
import { CONFIG } from '../api/config';

export default function Header({ userName, onSearch }) {
    // État local pour stocker le texte de recherche
    const [searchInput, setSearchInput] = useState("");
    const [cartItemCount, setCartItemCount] = useState(0);
    const userId = localStorage.getItem('epigreen_user_id');

    // --- Dropdown compte ---
    const [isAccountMenuOpen, setIsAccountMenuOpen] = useState(false);
    const menuRef = useRef(null);

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
        window.addEventListener('cartUpdated', fetchCartCount);

        return () => {
            window.removeEventListener('cartUpdated', fetchCartCount);
        };
    }, [userId]);

    // Fermer le menu si on clique en dehors
    useEffect(() => {
        const onClickOutside = (e) => {
            if (menuRef.current && !menuRef.current.contains(e.target)) {
                setIsAccountMenuOpen(false);
            }
        };

        const onEscape = (e) => {
            if (e.key === 'Escape') setIsAccountMenuOpen(false);
        };

        document.addEventListener('mousedown', onClickOutside);
        document.addEventListener('keydown', onEscape);

        return () => {
            document.removeEventListener('mousedown', onClickOutside);
            document.removeEventListener('keydown', onEscape);
        };
    }, []);

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

            {/* userName, Panier et Menu compte */}
            <div className="row" style={{ marginLeft: 'auto', gap: '20px', alignItems: 'center' }}>
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

                {/* Dropdown "Compte" */}
                <div ref={menuRef} style={{ position: 'relative' }}>
                    <button
                        className="small"
                        onClick={() => setIsAccountMenuOpen(v => !v)}
                        aria-haspopup="menu"
                        aria-expanded={isAccountMenuOpen}
                        style={{
                            background: '#666',
                            color: 'white',
                            display: 'flex',
                            alignItems: 'center',
                            gap: '8px'
                        }}
                    >
                        Compte <span style={{ fontSize: '12px' }}>▾</span>
                    </button>

                    {isAccountMenuOpen && (
                        <div
                            role="menu"
                            style={{
                                position: 'absolute',
                                right: 0,
                                top: 'calc(100% + 8px)',
                                minWidth: '180px',
                                background: 'white',
                                border: '1px solid rgba(0,0,0,0.12)',
                                borderRadius: '10px',
                                boxShadow: '0 8px 24px rgba(0,0,0,0.12)',
                                overflow: 'hidden',
                                zIndex: 9999
                            }}
                        >
                            <Link
                                to="/orders"
                                role="menuitem"
                                onClick={() => setIsAccountMenuOpen(false)}
                                style={{
                                    display: 'block',
                                    padding: '10px 12px',
                                    textDecoration: 'none',
                                    color: '#111'
                                }}
                            >
                                Mes commandes
                            </Link>

                            <div style={{ height: '1px', background: 'rgba(0,0,0,0.08)' }} />

                            <button
                                role="menuitem"
                                onClick={handleLogout}
                                style={{
                                    width: '100%',
                                    textAlign: 'left',
                                    padding: '10px 12px',
                                    background: 'transparent',
                                    border: 'none',
                                    cursor: 'pointer',
                                    color: '#111'
                                }}
                            >
                                Déconnexion
                            </button>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}