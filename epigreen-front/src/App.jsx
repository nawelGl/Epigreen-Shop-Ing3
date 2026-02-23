import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { useState } from 'react';
import Login from './pages/Login';
import Home from './pages/Home';
import ProductDetail from './pages/ProductDetail';
import Cart from './pages/Cart';
import './index.css';

export default function App() {
    // État d'authentification avec userId
    const [isAuth, setIsAuth] = useState(!!localStorage.getItem('epigreen_user_id'));

    return (


        <Router>
            <Routes>
                {/* Passage de la fonction pour mettre à jour l'état après login */}
                <Route path="/login" element={<Login onLoginSuccess={() => setIsAuth(true)} />} />
                
                {/* Redirection automatique selon isAuth */}
                <Route path="/" element={isAuth ? <Home /> : <Navigate to="/login" replace />} />
                {/* Navigation vers la page de détail des products (selon Id) */}
                <Route path="/cart" element={<Cart />} />
                <Route path="/products/:id" element={isAuth ? <ProductDetail /> : <Navigate to="/login" replace />} />
            </Routes>
        </Router>
    );
}