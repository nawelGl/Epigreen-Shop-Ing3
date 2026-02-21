import React, { useEffect, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import axios from 'axios';
import Header from '../components/Header';
import { CONFIG } from '../api/config';

export default function Cart() {
    const [cart, setCart] = useState(null);
    const [loading, setLoading] = useState(true);
    const navigate = useNavigate();
    
    const userName = localStorage.getItem('epigreen_user_name');
    const userId = localStorage.getItem('epigreen_user_id');

    // 1. Sécurité et récupération du panier
    useEffect(() => {
        if (!userId) {
            navigate('/login');
            return;
        }
        fetchCart();
    }, [userName, navigate]);

    const fetchCart = () => {
        setLoading(true);
        axios.get(`${CONFIG.API.CART}/${userId}`)
            .then(res => {
                setCart(res.data);
                setLoading(false);
            })
            .catch(err => {
                console.error("Erreur lors du chargement du panier :", err);
                setLoading(false);
            });
    };

    // 2. Vider le panier
    const handleClearCart = () => {
        if (window.confirm("Veux-tu vraiment vider ton panier ?")) {
            axios.delete(`${CONFIG.API.CART}/${userId}`)
                .then(() => {
                    setCart(null); // On vide l'affichage
                })
                .catch(err => console.error("Erreur lors de la suppression :", err));
        }
    };

    if (loading) return <div style={{ textAlign: 'center', marginTop: '50px' }}>Chargement du panier...</div>;

    // Vérification si le panier est vide (pas de panier du tout, ou liste d'items vide)
    const isCartEmpty = !cart || !cart.items || cart.items.length === 0;

    return (
        <div>
            <Header userName={userName} onSearch={() => navigate('/')} />
            
            <div className="container" style={{ maxWidth: '800px', margin: '0 auto', marginTop: '40px' }}>
                <h1 style={{ borderBottom: '2px solid var(--primary)', paddingBottom: '10px' }}>🛒 Ton Panier</h1>

                {isCartEmpty ? (
                    <div style={{ textAlign: 'center', padding: '50px', backgroundColor: '#f9f9f9', borderRadius: '8px', marginTop: '20px' }}>
                        <h3>Ton panier est tristement vide.</h3>
                        <p>Découvre nos nouvelles collections !</p>
                        <Link to="/">
                            <button style={{ padding: '10px 20px', backgroundColor: 'var(--primary)', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', marginTop: '10px' }}>
                                Continuer mes achats
                            </button>
                        </Link>
                    </div>
                ) : (
                    <div>
                        {/* Liste des articles */}
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '15px', marginTop: '20px' }}>
                            {cart.items.map((item, index) => (
                                <div key={index} className="card" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '15px' }}>
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
                                        <div style={{ width: '60px', height: '60px', backgroundColor: '#eee', borderRadius: '5px' }}></div>
                                        <div>
                                            <h3 style={{ margin: '0 0 5px 0' }}>{item.productName}</h3>
                                            <p style={{ margin: '0', color: '#666' }}>Prix unitaire : {item.price} €</p>
                                        </div>
                                    </div>
                                    
                                    <div style={{ textAlign: 'right' }}>
                                        <p style={{ margin: '0 0 5px 0', fontWeight: 'bold' }}>Qté: {item.quantity}</p>
                                        <p style={{ margin: '0', color: 'var(--primary)', fontWeight: 'bold', fontSize: '1.2rem' }}>
                                            {(item.price * item.quantity).toFixed(2)} €
                                        </p>
                                    </div>
                                </div>
                            ))}
                        </div>

                        {/* Total et Boutons d'action */}
                        <div style={{ marginTop: '30px', padding: '20px', backgroundColor: '#f0f4f8', borderRadius: '8px', textAlign: 'right' }}>
                            <h2 style={{ margin: '0 0 20px 0' }}>Total : <span style={{ color: 'var(--primary)' }}>{cart.totalPrice?.toFixed(2)} €</span></h2>
                            
                            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '15px' }}>
                                <button onClick={handleClearCart} style={{ padding: '12px 20px', backgroundColor: 'transparent', color: '#dc3545', border: '1px solid #dc3545', borderRadius: '5px', cursor: 'pointer', fontWeight: 'bold' }}>
                                    Vider le panier
                                </button>
                                
                                <button style={{ padding: '12px 30px', backgroundColor: '#28a745', color: 'white', border: 'none', borderRadius: '5px', cursor: 'pointer', fontSize: '1.1rem', fontWeight: 'bold' }}>
                                    Passer la commande 
                                </button>
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}