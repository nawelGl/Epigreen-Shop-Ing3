import React, { useEffect, useState } from "react";
import axios from "axios";
import { Link } from "react-router-dom";
import Header from "../components/Header";
import { CONFIG } from "../api/config";

export default function Home() {
    const [products, setProducts] = useState([]);
    const [searchTerm, setSearchTerm] = useState("");
    const userName = localStorage.getItem('epigreen_user_name');

    // Récupération des produits au chargement
    useEffect(() => {
        console.log(` Tentative d'appel vers : ${CONFIG.API.PRODUCT}/products`);
        
        axios.get(`${CONFIG.API.PRODUCT}/products`)
            .then(res => {
                console.log(" Données reçues du backend :", res.data);
                
                // Sécurité : Vérifier si c'est bien un tableau (Array)
                if (Array.isArray(res.data)) {
                    setProducts(res.data);
                } 
                // Si Spring Boot renvoie une pagination (Page<Product>)
                else if (res.data && Array.isArray(res.data.content)) {
                    setProducts(res.data.content);
                } 
                else {
                    console.error(" Format de données inattendu", res.data);
                    setProducts([]);
                }
            })
            .catch(err => {
                console.error(" Erreur de chargement :", err);
                setProducts([]); // Éviter le crash
            });
    }, []);

    // Sécurité : S'assurer que products est toujours un tableau
    const safeProducts = Array.isArray(products) ? products : [];

    // Filtre de recherche protégé contre les valeurs nulles
    const filteredProducts = safeProducts.filter(p => {
        if (!p) return false;
        const refMatch = p.reference ? p.reference.toLowerCase().includes(searchTerm.toLowerCase()) : false;
        const catMatch = p.category ? p.category.toLowerCase().includes(searchTerm.toLowerCase()) : false;
        return refMatch || catMatch;
    });

    const sections = ["Femme", "Homme", "Enfant"];

    return (
        <div>
            <Header userName={userName} onSearch={setSearchTerm} />

            <div className="container">
                {/* 1. Ligne de Recommandations */}
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

                {/* 2. Affichage par Catégories / Sections */}
                {sections.map(section => {
                    const sectionProducts = filteredProducts.filter(p => p.section === section);
                    if (sectionProducts.length === 0) return null;

                    return (
                        <div key={section} style={{ marginBottom: '50px' }}>
                            <h2 style={{ borderBottom: '2px solid var(--primary)', display: 'inline-block', paddingBottom: '5px' }}>
                                Section {section}
                            </h2>
                            
                            <div style={{
                                display: "grid",
                                gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))",
                                gap: "20px",
                                marginTop: "15px"
                            }}>
                                {sectionProducts.map((p) => (
                                    <Link key={p.idProduct} to={`/products/${p.idProduct}`}>
                                        <div className="card">
                                            <div style={{ backgroundColor: "#eee", height: "160px", borderRadius: "8px" }} />
                                            <div style={{ padding: '8px 0' }}>
                                                <h3 style={{ margin: '0', fontSize: '1.1rem' }}>{p.category}</h3>
                                                <p className="small">{p.reference}</p>
                                                <div className="row" style={{ justifyContent: 'space-between', marginTop: '10px' }}>
                                                    <strong style={{ color: 'var(--primary)' }}>{p.price} €</strong>
                                                    <span className="badge">{p.size}</span>
                                                </div>
                                            </div>
                                        </div>
                                    </Link>
                                ))}
                            </div>
                        </div>
                    );
                })}

                {filteredProducts.length === 0 && (
                    <p style={{ textAlign: 'center', marginTop: '50px' }}>Aucun produit trouvé.</p>
                )}
            </div>
        </div>
    );
}