import React, { useEffect, useRef, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import axios from 'axios';
import Header from '../components/Header';
import { CONFIG } from '../api/config';

import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

import L from 'leaflet';
import markerIcon from 'leaflet/dist/images/marker-icon.png';
import markerShadow from 'leaflet/dist/images/marker-shadow.png';

const DefaultIcon = L.icon({
    iconUrl: markerIcon,
    shadowUrl: markerShadow,
    iconSize: [25, 41],
    iconAnchor: [12, 41],
});
L.Marker.prototype.options.icon = DefaultIcon;

export default function Tracker() {
    const { id } = useParams();
    const [delivery, setDelivery] = useState(null);
    const [loading, setLoading] = useState(true);

    const [mapCenter, setMapCenter] = useState(null);

    const markerRef = useRef(null);

    const userName = localStorage.getItem('epigreen_user_name');

    useEffect(() => {
        let intervalId;

        const fetchDelivery = async () => {
            try {
                const res = await axios.get(`${CONFIG.API.DELIVERY}/${id}`);
                const d = res.data;
                setDelivery(d);
                setLoading(false);

                // setMapCenter une seule fois
                if (!mapCenter) {
                    // si tu as destLat/destLon dans l'API, c’est idéal pour centrer la carte
                    const centerLat =
                        d.destLat ?? d.destinationLat ?? d.currentLat ?? 48.8566;
                    const centerLon =
                        d.destLon ?? d.destinationLon ?? d.currentLon ?? 2.3522;

                    // auto-correction si inversé
                    let lat = centerLat;
                    let lon = centerLon;
                    if (lat != null && lon != null && Math.abs(lat) < 10 && Math.abs(lon) > 10) {
                        [lat, lon] = [lon, lat];
                    }

                    setMapCenter([lat, lon]);
                }
            } catch (err) {
                console.error('Erreur de récupération de la livraison:', err);
                setLoading(false);
            }
        };

        fetchDelivery();
        intervalId = setInterval(fetchDelivery, 3000);

        return () => clearInterval(intervalId);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [id]);

    // Déplacer le marker sans recréer la map
    useEffect(() => {
        if (!delivery) return;

        // coords actuelles (avec auto-fix lat/lon inversé)
        let lat = delivery.currentLat ?? 48.8566;
        let lon = delivery.currentLon ?? 2.3522;

        if (lat != null && lon != null && Math.abs(lat) < 10 && Math.abs(lon) > 10) {
            [lat, lon] = [lon, lat];
        }

        if (markerRef.current && typeof markerRef.current.setLatLng === 'function') {
            markerRef.current.setLatLng([lat, lon]);
        }
    }, [delivery?.currentLat, delivery?.currentLon, delivery]);

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

    if (loading) {
        return <div style={{ textAlign: 'center', marginTop: '50px' }}>Chargement du suivi...</div>;
    }

    if (!delivery) {
        return <div style={{ textAlign: 'center', marginTop: '50px' }}>Livraison introuvable.</div>;
    }

    // coordonnées actuelles (affichage marker)
    let currentLat = delivery.currentLat ?? 48.8566;
    let currentLon = delivery.currentLon ?? 2.3522;

    // auto-fix si inversé
    if (currentLat != null && currentLon != null && Math.abs(currentLat) < 10 && Math.abs(currentLon) > 10) {
        [currentLat, currentLon] = [currentLon, currentLat];
    }

    return (
        <div>
            <Header userName={userName} />

            <div className="container" style={{ maxWidth: '800px', margin: '40px auto', padding: '20px' }}>
                <h1 style={{ textAlign: 'center', marginBottom: '10px' }}>Suivi de Livraison</h1>

                <p style={{ textAlign: 'center', color: '#666', marginBottom: '30px' }}>
                    Numéro de suivi : <strong>{delivery.trackingNumber}</strong>
                </p>

                {/* Score éco */}
                <div style={{
                    backgroundColor: '#f0fdf4',
                    padding: '30px',
                    borderRadius: '12px',
                    marginBottom: '30px',
                    border: '2px solid #bbf7d0',
                    textAlign: 'center'
                }}>
                    <h2 style={{ marginTop: 0, color: '#166534' }}>Bilan Écologique de votre livraison</h2>

                    <div style={{
                        display: 'flex',
                        justifyContent: 'center',
                        alignItems: 'center',
                        gap: '30px',
                        margin: '20px 0'
                    }}>
                        <div style={{
                            width: '80px',
                            height: '80px',
                            borderRadius: '50%',
                            backgroundColor: getScoreColor(delivery.score),
                            color: 'white',
                            display: 'flex',
                            justifyContent: 'center',
                            alignItems: 'center',
                            fontSize: '2.5rem',
                            fontWeight: 'bold',
                            boxShadow: '0 4px 6px rgba(0,0,0,0.1)'
                        }}>
                            {delivery.score || '?'}
                        </div>

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

                {/* Détails livraison */}
                <div style={{
                    backgroundColor: 'white',
                    padding: '20px',
                    borderRadius: '8px',
                    border: '1px solid #ddd',
                    display: 'flex',
                    justifyContent: 'space-between'
                }}>
                    <div>
                        <h3 style={{ marginTop: 0, color: '#333' }}>Adresse de destination</h3>
                        <p style={{ margin: '5px 0' }}>{delivery.destinationStreet || delivery.destStreet}</p>
                        <p style={{ margin: '5px 0' }}>
                            {delivery.destinationZipCode || delivery.destZipCode} {delivery.destinationCity || delivery.destCity}
                        </p>
                    </div>

                    <div style={{ textAlign: 'right' }}>
                        <h3 style={{ marginTop: 0, color: '#333' }}>Statut</h3>
                        <span style={{
                            padding: '8px 15px',
                            borderRadius: '20px',
                            fontWeight: 'bold',
                            backgroundColor: delivery.status === 'PENDING' ? '#e2e8f0' : '#bbf7d0',
                            color: delivery.status === 'PENDING' ? '#475569' : '#166534'
                        }}>
                            {delivery.status === 'PENDING' ? 'En préparation' : delivery.status}
                        </span>
                    </div>
                </div>

                {/* MAP Leaflet (fluide) */}
                {delivery.status === 'IN_TRANSIT' && mapCenter && (
                    <div style={{ marginTop: '20px', borderRadius: '12px', overflow: 'hidden', border: '1px solid #ddd' }}>
                        <h3 style={{ padding: '10px', margin: 0, backgroundColor: '#f8f9fa', fontSize: '1rem', textAlign: 'center' }}>
                            📍 Position de votre colis en temps réel
                        </h3>

                        <MapContainer
                            center={mapCenter}
                            zoom={13}
                            style={{ height: '350px', width: '100%' }}
                        >
                            <TileLayer
                                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                                attribution='&copy; OpenStreetMap contributors'
                            />

                            <Marker
                                position={[currentLat, currentLon]}
                                ref={markerRef}
                            >
                                <Popup>Votre colis est ici ! 📦</Popup>
                            </Marker>
                        </MapContainer>
                    </div>
                )}

                {/* Message livré */}
                {delivery.status === 'DELIVERED' && (
                    <div style={{
                        marginTop: '20px',
                        backgroundColor: '#dcfce7',
                        color: '#166534',
                        padding: '15px',
                        borderRadius: '8px',
                        textAlign: 'center',
                        fontWeight: 'bold',
                        border: '1px solid #bbf7d0'
                    }}>
                        ✅ Votre colis a été livré ! Un mail récapitulatif vous a été envoyé.
                    </div>
                )}

                {/* Retour */}
                <div style={{ textAlign: 'center', marginTop: '30px' }}>
                    <Link
                        to="/"
                        style={{
                            textDecoration: 'none',
                            padding: '12px 25px',
                            backgroundColor: 'var(--primary)',
                            color: 'white',
                            borderRadius: '5px',
                            fontWeight: 'bold'
                        }}
                    >
                        Retour à la boutique
                    </Link>
                </div>
            </div>
        </div>
    );
}