import React from 'react';
import { useNavigate } from 'react-router-dom';

const Home = () => {
  const navigate = useNavigate();
  const name = localStorage.getItem('epigreen_user_name');

  const logout = () => {
    localStorage.clear();
    navigate('/login');
  };

  return (
    <div style={{ padding: '20px' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', borderBottom: '1px solid #ccc' }}>
        <h1>🌿 Epigreen Shop</h1>
        <div>
          <span>Bonjour, {name} ! </span>
          <button onClick={logout}>Déconnexion</button>
        </div>
      </header>
      <main>
        <h2>Produits</h2>
        <p>Ici, vous verrez la liste des produits bientôt.</p>
      </main>
    </div>
  );
};

export default Home;