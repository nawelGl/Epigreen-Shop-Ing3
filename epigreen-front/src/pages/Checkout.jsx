import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import Header from '../components/Header';
import { CONFIG } from '../api/config';

export default function Checkout() {
    const navigate = useNavigate();
    const userId = localStorage.getItem('epigreen_user_id');
    const userName = localStorage.getItem('epigreen_user_name');

    const [selectedAddress, setSelectedAddress] = useState(true);
    const [deliveryMethod, setDeliveryMethod] = useState('DOMICILE');
    const [isLoading, setIsLoading] = useState(false);
    const [userAddress, setUserAddress] = useState(null);

    // 1. Récupérer l'adresse du client au chargement
    useEffect(() => {
        const fetchCustomerAddress = async () => {
            try {
                const res = await axios.get(`${CONFIG.API.CUSTOMER}/${userId}`);
                const customer = res.data;

                if (customer.savedAddresses && customer.savedAddresses.length > 0) {
                    const firstAddress = customer.savedAddresses[0];
                    setUserAddress({
                        street: firstAddress.street,
                        city: firstAddress.city,
                        zipCode: firstAddress.zipCode,
                        lat: firstAddress.lat || 48.8689,
                        lon: firstAddress.lon || 2.3301
                    });
                } else {
                    // Adresse par défaut au cas où le client n'a pas d'adresse en BDD
                    setUserAddress({
                        street: "10 Rue de la Paix",
                        city: "Paris",
                        zipCode: "75002",
                        lat: 48.8689,
                        lon: 2.3301
                    });
                }
            } catch (err) {
                console.error("Erreur récupération client:", err);
                // Sécurité : on met l'adresse par défaut même si l'API plante
                setUserAddress({
                    street: "10 Rue de la Paix",
                    city: "Paris",
                    zipCode: "75002",
                    lat: 48.8689,
                    lon: 2.3301
                });
            }
        };

        if (userId) fetchCustomerAddress();
    }, [userId]);

    // 2. Validation de la commande
    const handleCheckout = async () => {
        setIsLoading(true);

        try {
            const createPayload = {
                orderId: Math.floor(Math.random() * 10000),
                customerId: parseInt(userId),
                originWarehouseId: 1,
                originLat: 45.7640,
                originLon: 4.8357,
                destStreet: userAddress.street,
                destCity: userAddress.city,
                destZipCode: userAddress.zipCode,
                destLat: userAddress.lat,
                destLon: userAddress.lon
            };

            const createRes = await axios.post(`${CONFIG.API.DELIVERY}/create`, createPayload);
            const newDeliveryId = createRes.data.id;

            const checkoutPayload = {
                deliveryId: newDeliveryId,
                deliveryMethod: deliveryMethod
            };

            console.log("Payload envoyé:", createPayload);

            await axios.post(`${CONFIG.API.DELIVERY}/checkout`, checkoutPayload);
            await axios.delete(`${CONFIG.API.CART}/${userName}`);

            navigate(`/tracker/${newDeliveryId}`);

        } catch (error) {
            console.error("Erreur lors de la validation :", error);
            alert("Une erreur est survenue lors de la validation de la commande.");
            setIsLoading(false);
        }
    };

    if (!userAddress) return <div style={{ textAlign: 'center', marginTop: '50px' }}>Chargement de vos informations...</div>;

    return (
        <div>
            <Header userName={userName} />
            <div className="container" style={{ maxWidth: '800px', margin: '40px auto', padding: '20px' }}>
                <h1 style={{ textAlign: 'center', marginBottom: '30px' }}>Validation de la commande</h1>

                {/* 1. Choix de l'adresse */}
                <div style={{ backgroundColor: '#f9f9f9', padding: '20px', borderRadius: '8px', marginBottom: '20px', border: '1px solid #ddd' }}>
                    <h3 style={{ marginTop: 0 }}>1. Adresse de livraison</h3>

                    <div
                        style={{
                            padding: '15px',
                            border: selectedAddress ? '2px solid var(--primary)' : '1px solid #ccc',
                            borderRadius: '5px',
                            backgroundColor: selectedAddress ? '#f0fff4' : 'white',
                            cursor: 'pointer',
                            display: 'flex',
                            alignItems: 'center',
                            gap: '15px'
                        }}
                        onClick={() => setSelectedAddress(true)}
                    >
                        <input type="radio" checked={selectedAddress} readOnly style={{ width: '20px', height: '20px' }} />
                        <div>
                            <strong>Mon adresse principale</strong><br />
                            {userAddress.street}, {userAddress.zipCode} {userAddress.city} {/* Corrigé ici aussi */}
                        </div>
                    </div>

                    <div style={{ marginTop: '10px', fontStyle: 'italic', color: '#666', fontSize: '0.9rem' }}>
                        * L'ajout de nouvelles adresses est temporairement désactivé.
                    </div>
                </div>

                {/* 2. Mode de livraison */}
                <div style={{ backgroundColor: '#f9f9f9', padding: '20px', borderRadius: '8px', marginBottom: '30px', border: '1px solid #ddd' }}>
                    <h3 style={{ marginTop: 0 }}>2. Mode de livraison</h3>

                    <div style={{ display: 'flex', gap: '20px' }}>
                        <label style={{ flex: 1, padding: '15px', border: '1px solid #ccc', borderRadius: '5px', cursor: 'pointer', backgroundColor: deliveryMethod === 'DOMICILE' ? '#e6f2ff' : 'white', display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <input type="radio" name="method" value="DOMICILE" checked={deliveryMethod === 'DOMICILE'} onChange={(e) => setDeliveryMethod(e.target.value)} />
                            <span>Livraison à Domicile</span>
                        </label>

                        <label style={{ flex: 1, padding: '15px', border: '1px solid #ccc', borderRadius: '5px', cursor: 'pointer', backgroundColor: deliveryMethod === 'POINT_RELAIS' ? '#e6f2ff' : 'white', display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <input type="radio" name="method" value="POINT_RELAIS" checked={deliveryMethod === 'POINT_RELAIS'} onChange={(e) => setDeliveryMethod(e.target.value)} />
                            <span>Point Relais (Plus écologique 🌱)</span>
                        </label>
                    </div>
                </div>

                {/* Bouton de validation */}
                <button
                    onClick={handleCheckout}
                    disabled={isLoading || !selectedAddress}
                    style={{
                        width: '100%',
                        padding: '15px',
                        fontSize: '1.2rem',
                        fontWeight: 'bold',
                        backgroundColor: (isLoading || !selectedAddress) ? '#ccc' : '#28a745',
                        color: 'white',
                        border: 'none',
                        borderRadius: '8px',
                        cursor: (isLoading || !selectedAddress) ? 'not-allowed' : 'pointer',
                    }}
                >
                    {isLoading ? "Calcul de l'empreinte carbone..." : "Confirmer et Payer"}
                </button>
            </div>
        </div>
    );
}