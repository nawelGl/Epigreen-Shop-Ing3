import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import axios from 'axios';
import Header from '../components/Header';
import { CONFIG } from '../api/config';
import { trackEvent } from '../api/tracker';

export default function ProductDetail() {
    const { id } = useParams();
    const navigate = useNavigate();
    
    const [product, setProduct] = useState(null);
    const [quantity, setQuantity] = useState(1);
    const [isAdding, setIsAdding] = useState(false);
    const [successMessage, setSuccessMessage] = useState("");
    const [selectedSize, setSelectedSize] = useState("");

    const userName = localStorage.getItem('epigreen_user_name');
    const userId = localStorage.getItem('epigreen_user_id');


    // 1. Récupération des détails du produit
    useEffect(() => {
        axios.get(`${CONFIG.API.PRODUCT}/${id}`)
            .then(res => {
                setProduct(res.data);
                // Tracer le click de product
                trackEvent("CLICK", { productId: parseInt(id) });
                console.log("Click event est tracé, product :", id);
            })
            .catch(err => console.error("Erreur lors du chargement du produit:", err));
    }, [id]);

    // 2. Logique d'ajout au panier 
    const handleAddToCart = () => {
        if (!selectedSize) {
            alert("Veuillez sélectionner une taille !");
            return;
        }
        
        setIsAdding(true);
        setSuccessMessage("");

        const payload = {
            productId: parseInt(id),
            quantity: quantity,
            size: selectedSize
        };

        // Appel POST 
        axios.post(`${CONFIG.API.CART}/${userId}`, payload)
            .then(res => {
                console.log("Panier mis à jour :", res.data);
                setIsAdding(false);
                setSuccessMessage("Produit ajouté au panier avec succès ! ");

                //Tracer l'ajout de cart
                trackEvent("CART", { productId: parseInt(id), quantity: quantity });
                // Fait disparaître le message après 3 secondes
                setTimeout(() => setSuccessMessage(""), 3000);
            })
            .catch(err => {
                console.error("Erreur lors de l'ajout au panier :", err);
                setIsAdding(false);
                alert("Erreur: Impossible d'ajouter l'article au panier.");
            });
    };

    // Cas: pas de produits 
    if (!product) return (
        <div>
            <Header userName={userName} onSearch={() => {}} />
            <div className="container" style={{ textAlign: 'center', marginTop: '50px' }}>
                <h3>Chargement du produit...</h3>
            </div>
        </div>
    );

    const availableSizes = product.sizes ? product.sizes.split(',') : ['Unique'];


    return (
        <div>
            <Header userName={userName} onSearch={() => navigate('/')} />
            
            <div className="container" style={{ display: 'flex', gap: '40px', marginTop: '40px', flexWrap: 'wrap' }}>
                
                {/* Emplacement image */}
                <div style={{ flex: '1 1 300px', backgroundColor: '#f5f5f5', minHeight: '400px', borderRadius: '10px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    <span style={{ color: '#aaa' }}>Image non disponible</span>
                </div>
                
                {/* Détails du produit */}
                <div style={{ flex: '2 1 400px' }}>
                    <p style={{ textTransform: 'uppercase', color: '#888', letterSpacing: '1px', fontSize: '0.85rem', marginBottom: '5px' }}>
                        {product.mainCategory}
                    </p>
                    
                    {/* Variables name, reference, genderSegment */}
                    <h1 style={{ marginTop: '0', marginBottom: '10px' }}>{product.name}</h1>
                    <p style={{ color: '#666', marginBottom: '20px' }}>Réf: {product.reference}</p>
                    
                    <span className="badge" style={{ backgroundColor: '#eef2f5', color: '#333', padding: '6px 12px', borderRadius: '15px' }}>
                        {product.genderSegment}
                    </span>
                    
                    <h2 style={{ color: 'var(--primary)', fontSize: '2rem', margin: '20px 0' }}>{product.price} €</h2>
                    {/* Gestion NULL =>non specifié */}
                    <div className="card" style={{ marginBottom: '30px', backgroundColor: '#fafafa', border: '1px solid #eaeaea' }}>
                        <p><strong>Marque :</strong> {product.brand || 'Non spécifiée'}</p>
                        <p><strong>Type :</strong> {product.articleType || 'Non spécifié'}</p>
                        <p><strong>Couleur :</strong> {product.color || 'Non spécifiée'}</p>
                        <p><strong>Tailles disponibles :</strong> {product.sizes || 'Taille unique'}</p>

                        {product.scoreEc && <p><strong>Score Écologique :</strong> {product.scoreLabel} 🍃</p>}
                    </div>

                    {/* Sélecteur de size --- */}
                    <div style={{ marginBottom: '25px' }}>
                        <p style={{ fontWeight: 'bold' }}>Sélectionner une taille :</p>
                        <div style={{ display: 'flex', gap: '10px' }}>
                            {availableSizes.map(size => (
                                <button
                                    key={size}
                                    onClick={() => setSelectedSize(size.trim())}
                                    style={{
                                        padding: '10px 20px',
                                        border: selectedSize === size.trim() ? '2px solid green' : '1px solid #ccc',
                                        backgroundColor: selectedSize === size.trim() ? '#e6fffa' : 'white',
                                        borderRadius: '5px',
                                        cursor: 'pointer',
                                        color:'black',
                                        fontWeight: selectedSize === size.trim() ? 'bold' : 'normal'
                                    }}
                                >
                                    {size.trim()}
                                </button>
                            ))}
                        </div>
                    </div>
                    
                    {/* Sélecteur de quantité */}
                    <div style={{ display: 'flex', gap: '15px', marginBottom: '20px', alignItems: 'center' }}>
                        <label htmlFor="quantity" style={{ fontWeight: 'bold' }}>Quantité :</label>
                        <select 
                            id="quantity"
                            value={quantity} 
                            onChange={(e) => setQuantity(Number(e.target.value))}
                            style={{ padding: '10px', fontSize: '1.1rem', borderRadius: '5px', border: '1px solid #ccc', width: '80px' }}
                        >
                            {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(num => (
                                <option key={num} value={num}>{num}</option>
                            ))}
                        </select>
                    </div>
                    
                    {/* Bouton d'ajout */}
                    <button 
                        onClick={handleAddToCart} 
                        disabled={isAdding}
                        style={{ 
                            width: '100%', 
                            padding: '15px', 
                            fontSize: '1.2rem', 
                            fontWeight: 'bold',
                            backgroundColor: isAdding ? '#ccc' : 'var(--primary)',
                            color: 'white',
                            border: 'none',
                            borderRadius: '8px',
                            cursor: isAdding ? 'not-allowed' : 'pointer',
                            transition: 'background-color 0.2s'
                        }}
                    >
                        {isAdding ? "Ajout en cours..." : "Ajouter au panier"}
                    </button>

                    {/* Message de succès */}
                    {successMessage && (
                        <div style={{ marginTop: '15px', padding: '15px', backgroundColor: '#d4edda', color: '#155724', borderRadius: '5px', textAlign: 'center', fontWeight: 'bold' }}>
                            {successMessage}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}