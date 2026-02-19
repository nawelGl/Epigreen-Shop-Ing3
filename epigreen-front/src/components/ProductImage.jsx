import React from 'react';
import defaultImage from '../assets/default-product.png';

export default function ProductImage({ imageUrl }) {
    return (
        <div className="card" style={{ padding: '0', overflow: 'hidden' }}>
            <img 
                src={imageUrl || defaultImage} 
                alt="Produit" 
                style={{ width: '100%', height: 'auto', display: 'block' }} 
            />
        </div>
    );
}