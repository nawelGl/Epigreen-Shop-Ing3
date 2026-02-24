import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import axios from 'axios';
import Header from '../components/Header';
import { CONFIG } from '../api/config';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';
import markerIcon from 'leaflet/dist/images/marker-icon.png';
import markerShadow from 'leaflet/dist/images/marker-shadow.png';

let DefaultIcon = L.icon({
    iconUrl: markerIcon,
    shadowUrl: markerShadow,
    iconSize: [25, 41],
    iconAnchor: [12, 41]
});
L.Marker.prototype.options.icon = DefaultIcon;

export default function Tracker() {
    const { id } = useParams(); // Récupère l'ID dans l'URL (ex: /tracker/15)
    const [delivery, setDelivery] = useState(null);
    const [loading, setLoading] = useState(true);

    const userName = localStorage.getItem('epigreen_user_name');

    useEffect(() => {
        // Appelle le endpoint GET /api/delivery/{id} de ms-delivery
        const fetchDelivery = async () => {
            try {
                const res = await axios.get(`${CONFIG.API.DELIVERY}/${id}`);
                setDelivery(res.data);
                setLoading(false);
            } catch (err) {
                console.error("Erreur de récupération de la livraison:", err);
                setLoading(false);
            }
        };

        fetchDelivery();
    }, [id]);

    // Fonction pour déterminer la couleur du score
    const getScoreColor = (score) => {
        switch (score) {
            case 'A': return '#1e7b3e';
            case 'B': return '#85a438';
            case 'C': return '#f5c61a';
            case 'D': return '#e58025';
            case 'E': return '#d22c2a';
            default: return '#ccc';
        }
    };

    if (loading) return <div style={{ textAlign: 'center', marginTop: '50px' }}>Chargement du suivi...</div>;

    if (!delivery) return <div style={{ textAlign: 'center', marginTop: '50px' }}>Livraison introuvable.</div>;

    return (
        <div>
            <Header userName={userName} />
            <div className="container" style={{ maxWidth: '800px', margin: '40px auto', padding: '20px' }}>
                <h1 style={{ textAlign: 'center', marginBottom: '10px' }}>Suivi de Livraison</h1>
                <p style={{ textAlign: 'center', color: '#666', marginBottom: '30px' }}>
                    Numéro de suivi : <strong>{delivery.trackingNumber}</strong>
                </p>

                {/* Score éco */}
                <div style={{ backgroundColor: '#f0fdf4', padding: '30px', borderRadius: '12px', marginBottom: '30px', border: '2px solid #bbf7d0', textAlign: 'center' }}>
                    <h2 style={{ marginTop: 0, color: '#166534' }}>Bilan Écologique de votre livraison</h2>

                    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: '30px', margin: '20px 0' }}>
                        {/* Bulle Score */}
                        <div style={{
                            width: '80px', height: '80px', borderRadius: '50%',
                            backgroundColor: getScoreColor(delivery.score),
                            color: 'white', display: 'flex', justifyContent: 'center', alignItems: 'center',
                            fontSize: '2.5rem', fontWeight: 'bold', boxShadow: '0 4px 6px rgba(0,0,0,0.1)'
                        }}>
                            {delivery.score || '?'}
                        </div>

                        {/* Chiffres clés */}
                        <div style={{ textAlign: 'left' }}>
                            <p style={{ margin: '5px 0', fontSize: '1.2rem' }}>
                                📏 Distance estimée : <strong>{delivery.distanceKm} km</strong>
                            </p>
                            <p style={{ margin: '5px 0', fontSize: '1.2rem' }}>
                                ☁️ Empreinte CO2 : <strong>{delivery.carbonFootprint} kg</strong>
                            </p>
                        </div>
                    </div>
                    <p style={{ fontSize: '0.9rem', color: '#166534', fontStyle: 'italic', margin: 0 }}>
                        {delivery.deliveryMethod === 'POINT_RELAIS'
                            ? "Merci d'avoir choisi la livraison en Point Relais, vous avez réduit vos émissions !"
                            : "Astuce : La prochaine fois, choisissez la livraison en Point Relais pour améliorer ce score."}
                    </p>
                </div>

                {/* Détails de la livraison */}
                <div style={{ backgroundColor: 'white', padding: '20px', borderRadius: '8px', border: '1px solid #ddd', display: 'flex', justifyContent: 'space-between' }}>
                    <div>
                        <h3 style={{ marginTop: 0, color: '#333' }}>Adresse de destination</h3>
                        <p style={{ margin: '5px 0' }}>{delivery.destinationStreet}</p>
                        <p style={{ margin: '5px 0' }}>{delivery.destinationZipCode} {delivery.destinationCity}</p>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                        <h3 style={{ marginTop: 0, color: '#333' }}>Statut</h3>
                        <span style={{
                            padding: '8px 15px', borderRadius: '20px', fontWeight: 'bold',
                            backgroundColor: delivery.status === 'PENDING' ? '#e2e8f0' : '#bbf7d0',
                            color: delivery.status === 'PENDING' ? '#475569' : '#166534'
                        }}>
                            {delivery.status === 'PENDING' ? 'En préparation' : delivery.status}
                        </span>
                    </div>
                </div>

                {/* 1. La Map (s'affiche seulement si en transit) */}
                {delivery.status === 'IN_TRANSIT' && (
                    <div style={{ marginTop: '20px', borderRadius: '12px', overflow: 'hidden', border: '1px solid #ddd' }}>
                        <h3 style={{ padding: '10px', margin: 0, backgroundColor: '#f8f9fa', fontSize: '1rem', textAlign: 'center' }}>
                            📍 Position de votre colis en temps réel
                        </h3>
                        <MapContainer
                            center={[delivery.currentLat || 48.8566, delivery.currentLon || 2.3522]}
                            zoom={13}
                            style={{ height: '350px', width: '100%' }}
                        >
                            <TileLayer
                                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                            />

                            {/* Le marqueur du livreur */}
                            <Marker position={[delivery.currentLat || 48.8566, delivery.currentLon || 2.3522]}>
                                <Popup>
                                    Votre colis est ici ! 📦
                                </Popup>
                            </Marker>
                        </MapContainer>
                    </div>
                )}

                {/* 2. Message de succès (s'affiche seulement si livré) */}
                {delivery.status === 'DELIVERED' && (
                    <div style={{ marginTop: '20px', backgroundColor: '#dcfce7', color: '#166534', padding: '15px', borderRadius: '8px', textAlign: 'center', fontWeight: 'bold', border: '1px solid #bbf7d0' }}>
                        ✅ Votre colis a été livré ! Un mail récapitulatif vous a été envoyé.
                    </div>
                )}

                {/* Bouton retour */}
                <div style={{ textAlign: 'center', marginTop: '30px' }}>
                    <Link to="/" style={{ textDecoration: 'none', padding: '12px 25px', backgroundColor: 'var(--primary)', color: 'white', borderRadius: '5px', fontWeight: 'bold' }}>
                        Retour à la boutique
                    </Link>
                </div>
            </div>
        </div>
    );
}