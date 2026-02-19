import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import axios from 'axios';
import Header from '../components/Header';
import { CONFIG } from '../api/config';

export default function ProductDetail() {
    const { id } = useParams();
    const [product, setProduct] = useState(null);
    const userName = localStorage.getItem('epigreen_user_name');

    // Fetch details produit
    useEffect(() => {
        axios.get(`${CONFIG.API.PRODUCT}/products/${id}`)
            .then(res => setProduct(res.data))
            .catch(err => console.error(err));
    }, [id]);

    const handleAddToCart = () => {
        // TODO: Implémenter l'ajout au panier ms-cart
        alert(`Produit ajouté au panier !`);
    };

    if (!product) return <div className="container">Chargement...</div>;

    return (
        <div>
            <Header userName={userName} onSearch={() => {}} />
            
            <div className="container" style={{ display: 'flex', gap: '40px', marginTop: '40px', flexWrap: 'wrap' }}>
                <div style={{ flex: '1 1 300px', backgroundColor: '#eee', minHeight: '400px', borderRadius: '10px' }}>
                    {/* Emplacement image */}
                </div>
                
                <div style={{ flex: '2 1 400px' }}>
                    <h1>{product.category} - {product.reference}</h1>
                    <span className="badge">{product.section}</span>
                    
                    <h2 style={{ color: 'var(--primary)', margin: '20px 0' }}>{product.price} €</h2>
                    
                    <div className="card" style={{ marginBottom: '20px' }}>
                        <p><strong>Matière:</strong> {product.material}</p>
                        <p><strong>Couleur:</strong> {product.color}</p>
                        <p><strong>Taille:</strong> {product.size}</p>
                    </div>
                    
                    <button onClick={handleAddToCart} style={{ width: '100%', padding: '15px', fontSize: '1.1rem' }}>
                        Ajouter au panier
                    </button>
                </div>
            </div>
        </div>
    );
}