import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import axios from 'axios';
import Header from '../components/Header';
import { CONFIG } from '../api/config';

const Orders = () => {
    const [deliveries, setDeliveries] = useState([]);
    const userId = localStorage.getItem('epigreen_user_id');
    const userName = localStorage.getItem('epigreen_user_name');

    useEffect(() => {
        const fetchOrders = async () => {
            const id = localStorage.getItem('epigreen_user_id');
            if (!id) return;

            try {
                const response = await axios.get(`${CONFIG.API.DELIVERY}/customer/${id}`);
                setDeliveries(response.data);
            } catch (err) {
                console.error("Erreur lors de la récup des commandes", err);
            }
        };
        fetchOrders();
    }, []);

    return (
        <div>
            <Header userName={userName} />
            <div className="container" style={{ maxWidth: '900px', margin: '40px auto', padding: '0 20px' }}>
                <h1>Mes Commandes</h1>
                {deliveries.length === 0 ? (
                    <p>Vous n'avez pas encore de commande.</p>
                ) : (
                    <div style={{ display: 'grid', gap: '20px' }}>
                        {deliveries.map(order => (
                            <div key={order.id} style={{ border: '1px solid #ddd', borderRadius: '12px', padding: '20px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', backgroundColor: '#fff' }}>
                                <div>
                                    <p style={{ margin: 0, fontWeight: 'bold', fontSize: '1.1rem' }}>📦 Commande #{order.trackingNumber}</p>
                                    <p style={{ margin: '5px 0', color: '#666' }}>Destination : {order.destinationCity}</p>
                                    <span style={{
                                        padding: '4px 10px', borderRadius: '15px', fontSize: '0.8rem', fontWeight: 'bold',
                                        backgroundColor: order.status === 'DELIVERED' ? '#dcfce7' : '#fff3cd',
                                        color: order.status === 'DELIVERED' ? '#166534' : '#856404'
                                    }}>
                                        {order.status}
                                    </span>
                                </div>
                                <Link to={`/tracker/${order.id}`} style={{ backgroundColor: '#28a745', color: 'white', padding: '10px 20px', borderRadius: '5px', textDecoration: 'none', fontWeight: 'bold' }}>
                                    Suivre
                                </Link>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
};

export default Orders;