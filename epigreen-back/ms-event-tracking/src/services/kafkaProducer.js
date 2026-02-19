const { Kafka } = require('kafkajs');
require('dotenv').config();

// Configuration du client Kafka pointant vers la VM
const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'tracker-service',
    brokers: [process.env.KAFKA_BROKER] 
});

const producer = kafka.producer();

// Fonction pour connecter le producteur au démarrage du serveur
const connectProducer = async () => {
    try {
        await producer.connect();
        console.log("====== Connecté au broker Kafka sur la VM =====");
    } catch (error) {
        console.error("Erreur de connexion à Kafka:", error);
    }
};

// Fonction pour envoyer l'événement dans le bon topic
const sendEvent = async (eventData) => {
    try {
        let topic = '';
        switch (eventData.eventType) {
            case 'CLICK':
                topic = 'user-event-click';
                break;
            case 'CART':
                topic = 'user-event-cart';
                break;
            case 'SEARCH':
                topic = 'user-event-search';
                break;
            default:
                console.warn(`⚠️ Type d'événement ignoré (aucun topic correspondant): ${eventData.eventType}`);
                return; // On n'envoie rien si le type est inconnu
        }

        // Envoi du message JSON dans Kafka
        await producer.send({
            topic: topic,
            messages: [
                { value: JSON.stringify(eventData) }
            ],
        });
        
        console.log(`➡️ Événement [${eventData.eventType}] envoyé au topic [${topic}]`);
    } catch (error) {
        console.error(` Échec de l'envoi au topic Kafka:`, error);
    }
};

module.exports = { connectProducer, sendEvent };