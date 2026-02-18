import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Login from './pages/Login';
import Home from './pages/Home';

function App() {
  // vérifer si utilisateur est connecté
  const isAuthenticated = () => !!localStorage.getItem('epigreen_user_id');

  return (
    <Router>
      <Routes>
        {/* page login */}
        <Route path="/login" element={<Login />} />
        
        {/* page main (si pas encore de connexion, aller à la page login obligatoirement */}
        <Route 
          path="/" 
          element={isAuthenticated() ? <Home /> : <Navigate to="/login" />} 
        />
      </Routes>
    </Router>
  );
}

export default App;