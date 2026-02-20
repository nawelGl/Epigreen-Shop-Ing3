import React, { useEffect, useState } from "react";
import axios from "axios";
import { Link } from "react-router-dom";
import Header from "../components/Header";
import { CONFIG } from "../api/config";

export default function Home() {
    const [products, setProducts] = useState([]);
    const [searchTerm, setSearchTerm] = useState("");
    
    // états pour les onglets et la pagination
    const [activeTab, setActiveTab] = useState("Women");
    const [currentPage, setCurrentPage] = useState(1);
    const itemsPerPage = 12; // Nombre de produits affichés par page

    const userName = localStorage.getItem('epigreen_user_name');

    // Récupération des produits au chargement
    useEffect(() => {
        // On force le backend à renvoyer jusqu'à 200 produits d'un coup grâce à ?size=200
        axios.get(`${CONFIG.API.PRODUCT}?size=200`)
            .then(res => {
                if (Array.isArray(res.data)) {
                    setProducts(res.data);
                } 
                else if (res.data && Array.isArray(res.data.content)) {
                    setProducts(res.data.content);
                } 
                else {
                    setProducts([]);
                }
            })
            .catch(err => {
                console.error(" Erreur de chargement :", err);
                setProducts([]); 
            });
    }, []);

    const safeProducts = Array.isArray(products) ? products : [];

    // Filtre de recherche
    const filteredProducts = safeProducts.filter(p => {
        if (!p) return false;
        const refMatch = p.reference ? p.reference.toLowerCase().includes(searchTerm.toLowerCase()) : false;
        const nameMatch = p.name ? p.name.toLowerCase().includes(searchTerm.toLowerCase()) : false;
        return refMatch || nameMatch;
    });

    // Filtre des produits pour l'onglet actuellement sélectionné
    const activeTabProducts = filteredProducts.filter(p => 
        p.genderSegment && p.genderSegment.trim().toLowerCase() === activeTab.toLowerCase()
    );

    // Calculs pour la pagination 
    const indexOfLastItem = currentPage * itemsPerPage;
    const indexOfFirstItem = indexOfLastItem - itemsPerPage;
    const currentProducts = activeTabProducts.slice(indexOfFirstItem, indexOfLastItem);
    const totalPages = Math.ceil(activeTabProducts.length / itemsPerPage);

    const sections = ["Women", "Men", "Unisex", "Girls", "Boys"];

    // Fonction pour changer d'onglet (et remettre la page à 1)
    const handleTabChange = (section) => {
        setActiveTab(section);
        setCurrentPage(1);
    };

    return (
        <div>
            <Header userName={userName} onSearch={setSearchTerm} />

            <div className="container">
                <h2 style={{ color: 'var(--primary)', marginTop: '0' }}>✨ Recommandations pour vous</h2>
                <div className="row" style={{ gap: '20px', flexWrap: 'nowrap', overflowX: 'auto', paddingBottom: '10px' }}>
                    {[1, 2, 3, 4].map((item) => (
                        <div key={`rec-${item}`} className="card" style={{ minWidth: '220px', backgroundColor: '#fafafa', borderStyle: 'dashed', borderColor: '#ccc' }}>
                            <div style={{ height: '120px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#aaa' }}>
                                Bientôt disponible...
                            </div>
                        </div>
                    ))}
                </div>

                <hr style={{ border: '0', borderTop: '1px solid var(--border)', margin: '30px 0' }} />

                {/* --- BARRE D'ONGLETS --- */}
                <div style={{ display: 'flex', gap: '20px', marginBottom: '30px', borderBottom: '2px solid #eee', overflowX: 'auto' }}>
                    {sections.map(section => (
                        <button
                            key={section}
                            onClick={() => handleTabChange(section)}
                            style={{
                                background: 'none',
                                border: 'none',
                                padding: '10px 5px',
                                fontSize: '1.1rem',
                                cursor: 'pointer',
                                fontWeight: activeTab === section ? 'bold' : 'normal',
                                color: activeTab === section ? 'var(--primary)' : '#777',
                                borderBottom: activeTab === section ? '3px solid var(--primary)' : '3px solid transparent',
                                marginBottom: '-2px', 
                                transition: 'all 0.2s'
                            }}
                        >
                            {section}
                        </button>
                    ))}
                </div>

               {/* --- AFFICHAGE DES PRODUITS DE L'ONGLET --- */}
               <div style={{ 
                    display: "grid", 
                    gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", 
                    gap: "25px",
                    marginTop: "20px"
                }}>
                    {currentProducts.map((p) => (
                        <Link key={p.id} to={`/products/${p.id}`} style={{ textDecoration: 'none', color: 'inherit', display: 'block', height: '100%' }}>
                            <div className="card" style={{ 
                                display: 'flex', 
                                flexDirection: 'column', 
                                height: '100%', /* toutes les cartes à la même taille */
                                padding: '15px',
                                boxSizing: 'border-box',
                                transition: 'transform 0.2s, box-shadow 0.2s'
                            }} 
                            onMouseOver={e => { e.currentTarget.style.transform = 'translateY(-5px)'; e.currentTarget.style.boxShadow = '0 8px 16px rgba(0,0,0,0.1)'; }} 
                            onMouseOut={e => { e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = 'none'; }}>
                                
                                {/* Placeholder de l'image (fixe) */}
                                <div style={{ backgroundColor: "#f0f0f0", height: "200px", borderRadius: "8px", marginBottom: "15px", width: "100%", flexShrink: 0 }} />
                                
                                {/* Conteneur du texte (flexible) */}
                                <div style={{ display: 'flex', flexDirection: 'column', flexGrow: 1 }}>
                                    
                                    <p style={{ margin: '0 0 5px 0', fontSize: '0.75rem', color: '#888', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
                                        {p.mainCategory}
                                    </p>
                                    
                                
                                    <h3 style={{ 
                                        margin: '0 0 10px 0', 
                                        fontSize: '1.1rem', 
                                        display: '-webkit-box', 
                                        WebkitLineClamp: 2, 
                                        WebkitBoxOrient: 'vertical', 
                                        overflow: 'hidden',
                                        lineHeight: '1.3'
                                    }}>
                                        {p.name}
                                    </h3>
                                    
                                    <p className="small" style={{ color: '#666', marginBottom: '10px' }}>{p.reference}</p>
                                    
                                    {/* Le bas de la carte avec le prix et la taille */}
                                        <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', marginTop: 'auto' }}>
                                            <strong style={{ color: 'var(--primary)', fontSize: '1.2rem' }}>{p.price} €</strong>
                            
                                            <span className="badge" style={{ 
                                                backgroundColor: '#f0f0f0', 
                                                color: '#333', 
                                                padding: '4px 8px', 
                                                borderRadius: '4px', 
                                                fontSize: '0.8rem',
                                                maxWidth: '120px', 
                                                whiteSpace: 'nowrap', 
                                                overflow: 'hidden', // on cache si la taille de texte dépasse
                                                textOverflow: 'ellipsis' 
                                            }}>
                                                {p.sizes}
                                            </span>
                                        </div>

                                </div>
                            </div>
                        </Link>
                    ))}
                </div>

                {/* --- CONTRÔLES DE PAGINATION --- */}
                {totalPages > 1 && (
                    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: '20px', marginTop: '40px' }}>
                        <button 
                            onClick={() => setCurrentPage(prev => prev - 1)} 
                            disabled={currentPage === 1}
                            style={{ padding: '8px 16px', borderRadius: '4px', border: '1px solid #ccc', background: currentPage === 1 ? '#f9f9f9' : 'white', cursor: currentPage === 1 ? 'not-allowed' : 'pointer' }}
                        >
                            Précédent
                        </button>
                        
                        <span style={{ fontSize: '1rem', color: '#555' }}>
                            Page <strong>{currentPage}</strong> sur {totalPages}
                        </span>
                        
                        <button 
                            onClick={() => setCurrentPage(prev => prev + 1)} 
                            disabled={currentPage === totalPages}
                            style={{ padding: '8px 16px', borderRadius: '4px', border: '1px solid #ccc', background: currentPage === totalPages ? '#f9f9f9' : 'white', cursor: currentPage === totalPages ? 'not-allowed' : 'pointer' }}
                        >
                            Suivant
                        </button>
                    </div>
                )}

                {activeTabProducts.length === 0 && (
                    <div style={{ textAlign: 'center', marginTop: '50px', padding: '40px', backgroundColor: '#f9f9f9', borderRadius: '8px' }}>
                        <h3 style={{ color: '#777' }}>Aucun produit trouvé.</h3>
                        <p style={{ color: '#999' }}>Pas d'articles dans la section "{activeTab}" pour le moment.</p>
                    </div>
                )}
            </div>
        </div>
    );
}