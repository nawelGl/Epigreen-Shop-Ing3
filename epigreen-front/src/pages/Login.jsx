import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { CONFIG } from '../api/config';

export default function Login({ onLoginSuccess }) {
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const navigate = useNavigate();

    const handleLogin = async () => {
        try {
            const res = await fetch(`${CONFIG.API.CUSTOMER}/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email, password })
            });

            if (res.ok) {
                const data = await res.json();
                
                // Sauvegarde
                localStorage.setItem('epigreen_user_id', data.id);
                localStorage.setItem('epigreen_user_name', data.firstName);
                
                // Forcer la maj de App.jsx
                if (onLoginSuccess) onLoginSuccess();
                navigate('/');
            } else {
                alert("Email ou mot de passe incorrect.");
            }
        } catch (err) {
            alert("Erreur serveur.");
        }
    };

    return (
        <div className="container" style={{ maxWidth: '400px', marginTop: '100px' }}>
            <div className="card">
                <div className="brand" style={{ textAlign: 'center', fontSize: '2rem' }}>🌿 Epigreen</div>
                <h2 style={{ textAlign: 'center' }}>Connexion</h2>
                <input type="email" placeholder="Email" onChange={(e) => setEmail(e.target.value)} />
                <input type="password" placeholder="Mot de passe" onChange={(e) => setPassword(e.target.value)} />
                <button onClick={handleLogin}>Se connecter</button>
            </div>
        </div>
    );
}