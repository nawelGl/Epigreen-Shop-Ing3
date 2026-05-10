import React, { useState, useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import axios from 'axios';
import { CONFIG } from '../api/config';
import { trackEvent } from '../api/tracker';

export default function Header({ userName, onSearch }) {
    // --- États existants ---
    const [searchInput, setSearchInput] = useState("");
    const [cartItemCount, setCartItemCount] = useState(0);
    const userId = localStorage.getItem('epigreen_user_id');

    // --- États pour le menu Compte ---
    const [isAccountMenuOpen, setIsAccountMenuOpen] = useState(false);
    const menuRef = useRef(null);

    const [notifications, setNotifications] = useState([]);
    const [unreadCount, setUnreadCount] = useState(0);
    const [isNotifMenuOpen, setIsNotifMenuOpen] = useState(false);
    const notifRef = useRef(null);

    useEffect(() => {
        if (!userId) return;

        let ws;
        let reconnectTimeout;

        // On encapsule dans une fonction async pour gérer l'API puis le WebSocket
        const initNotifications = async () => {
            // --- 1. LE RATTRAPAGE (Base de données) ---
            try {
                const res = await axios.get(`http://localhost:8089/api/notifications/unread/${userId}`);
                setNotifications(res.data);
                setUnreadCount(res.data.length); // Met à jour la cloche
            } catch (error) {
                console.error("Erreur de récupération de l'historique :", error);
            }

            // --- 2. LE FLUX LIVE (WebSocket) ---
            const connectWebSocket = () => {
                ws = new WebSocket(`${CONFIG.API.NOTIFICATION_WS}?userId=${userId}`);

                ws.onopen = () => console.log("✅ Connecté au serveur de notifications temps réel (Kong) !");

                ws.onmessage = (event) => {
                    const newNotification = JSON.parse(event.data);
                    console.log("🔔 Nouvelle notification reçue :", newNotification);

                    // Ajout au début de la liste et incrémentation de la cloche
                    setNotifications(prev => [newNotification, ...prev]);
                    setUnreadCount(prev => prev + 1);
                };

                ws.onclose = () => {
                    console.warn("WebSocket déconnectée. Reconnexion dans 3s...");
                    reconnectTimeout = setTimeout(connectWebSocket, 3000);
                };

                ws.onerror = (err) => {
                    console.error("Erreur WebSocket :", err);
                    ws.close();
                };
            };

            connectWebSocket();
        };

        initNotifications();

        return () => {
            clearTimeout(reconnectTimeout);
            if (ws) {
                ws.onclose = null;
                ws.close();
            }
        };
    }, [userId]);


    // --- Fonctions existantes ---
    const handleLogout = () => {
        localStorage.clear();
        window.location.href = '/login';
    };

    const handleSearchClick = () => {
        if (onSearch) onSearch(searchInput);
        trackEvent('SEARCH', { keyword: searchInput });
    };

    const handleKeyDown = (e) => {
        if (e.key === 'Enter') handleSearchClick();
    };

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
            .catch(err => setCartItemCount(0));
    };

    const handleClearNotifications = async () => {
        try {
            await axios.post(`http://localhost:8089/api/notifications/mark-as-read/${userId}`);
            // On peut vider la liste ou juste fermer le menu
            setNotifications([]);
            setIsNotifMenuOpen(false);
            setUnreadCount(0);
        } catch (error) {
            console.error("Erreur lors du nettoyage en base:", error);
        }
    };

    useEffect(() => {
        fetchCartCount();
        window.addEventListener('cartUpdated', fetchCartCount);
        return () => window.removeEventListener('cartUpdated', fetchCartCount);
    }, [userId]);

    useEffect(() => {
        const onClickOutside = (e) => {
            if (menuRef.current && !menuRef.current.contains(e.target)) setIsAccountMenuOpen(false);
            if (notifRef.current && !notifRef.current.contains(e.target)) setIsNotifMenuOpen(false);
        };
        document.addEventListener('mousedown', onClickOutside);
        return () => document.removeEventListener('mousedown', onClickOutside);
    }, []);

    const getStatusMessage = (status) => {
        switch (status) {
            case 'DELIVERED':
                return "Votre colis a été livré avec succès ! ✅";
            case 'IN_TRANSIT':
                return "Votre colis est en cours d'acheminement. 🚚";
            case 'PENDING':
                return "Votre commande est en préparation. 📦";
            default:
                return "Le statut de votre commande a été mis à jour.";
        }
    };

    return (
        <div className="topbar">
            <Link to="/" className="brand">🌿 Epigreen</Link>

            <div style={{ flex: 1, display: 'flex', justifyContent: 'center', gap: '5px' }}>
                <input
                    type="text"
                    placeholder="Rechercher..."
                    style={{ width: '100%', maxWidth: '300px' }}
                    value={searchInput}
                    onChange={(e) => setSearchInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                />
                <button onClick={handleSearchClick} style={{ padding: '10px 15px' }}>Rechercher</button>
            </div>

            <div className="row" style={{ marginLeft: 'auto', gap: '20px', alignItems: 'center' }}>
                <span style={{ fontWeight: 'bold' }}>Bonjour {userName || 'Invité'} !</span>

                {/* BOUTON CLOCHE DE NOTIFICATIONS */}
                <div ref={notifRef} style={{ position: 'relative' }}>
                    <button
                        style={{ background: 'none', border: 'none', fontSize: '24px', cursor: 'pointer', position: 'relative' }}
                        onClick={() => {
                            setIsNotifMenuOpen(!isNotifMenuOpen);
                            //if (!isNotifMenuOpen) setUnreadCount(0); // On remet le compteur à zéro quand on lit
                        }}
                    >
                        🔔
                        {unreadCount > 0 && (
                            <span className="badge" style={{
                                position: 'absolute', top: '-5px', right: '-5px',
                                background: '#dc3545', color: 'white', border: 'none', fontSize: '11px',
                                padding: '2px 6px', borderRadius: '50%'
                            }}>
                                {unreadCount}
                            </span>
                        )}
                    </button>

                    {/* Le Dropdown des notifications */}
                    {isNotifMenuOpen && (
                        <div style={{
                            position: 'absolute', right: 0, top: 'calc(100% + 15px)',
                            width: '350px', background: 'white', border: '1px solid rgba(0,0,0,0.12)',
                            borderRadius: '10px', boxShadow: '0 8px 24px rgba(0,0,0,0.12)',
                            maxHeight: '400px', overflowY: 'auto', zIndex: 9999
                        }}>
                            <div style={{ padding: '15px', fontWeight: 'bold', borderBottom: '1px solid #eee', backgroundColor: '#f8f9fa', borderRadius: '10px 10px 0 0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <span>Vos Notifications</span>
                                {/* Le fameux bouton pour valider la lecture en base */}
                                {notifications.length > 0 && (
                                    <button onClick={handleClearNotifications} style={{ background: 'none', border: 'none', color: '#007bff', fontSize: '0.8rem', cursor: 'pointer' }}>
                                        Tout marquer comme lu
                                    </button>
                                )}
                            </div>

                            {notifications.length === 0 ? (
                                <div style={{ padding: '20px', color: '#666', textAlign: 'center' }}>
                                    Aucune nouvelle notification.
                                </div>
                            ) : (
                                notifications.map((notif, index) => (
                                    <div key={index} style={{ padding: '15px', borderBottom: '1px solid #eee', fontSize: '0.9rem', display: 'flex', gap: '10px', alignItems: 'start' }}>
                                        <span style={{ fontSize: '1.2rem' }}>📦</span>
                                        <div>
                                            {/* On affiche le champ 'message' généré par ton backend */}
                                            <strong style={{ display: 'block', color: '#333' }}>
                                                {notif.message}
                                            </strong>
                                            {/* On affiche l'heure si tu l'as enregistrée */}
                                            {notif.createdAt && (
                                                <span style={{ color: '#999', fontSize: '0.8rem' }}>
                                                    {new Date(notif.createdAt).toLocaleTimeString()}
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                ))
                            )}
                        </div>
                    )}
                </div>

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

                {/* Menu Compte */}
                <div ref={menuRef} style={{ position: 'relative' }}>
                    <button
                        className="small"
                        onClick={() => setIsAccountMenuOpen(v => !v)}
                        style={{ background: '#666', color: 'white', display: 'flex', alignItems: 'center', gap: '8px' }}
                    >
                        Compte <span style={{ fontSize: '12px' }}>▾</span>
                    </button>
                    {isAccountMenuOpen && (
                        <div style={{
                            position: 'absolute', right: 0, top: 'calc(100% + 8px)',
                            minWidth: '180px', background: 'white', border: '1px solid rgba(0,0,0,0.12)',
                            borderRadius: '10px', boxShadow: '0 8px 24px rgba(0,0,0,0.12)', zIndex: 9999
                        }}>
                            <Link to="/orders" onClick={() => setIsAccountMenuOpen(false)} style={{ display: 'block', padding: '10px 12px', textDecoration: 'none', color: '#111' }}>
                                Mes commandes
                            </Link>
                            <div style={{ height: '1px', background: 'rgba(0,0,0,0.08)' }} />
                            <button onClick={handleLogout} style={{ width: '100%', textAlign: 'left', padding: '10px 12px', background: 'transparent', border: 'none', cursor: 'pointer', color: '#111' }}>
                                Déconnexion
                            </button>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}