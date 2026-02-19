const { sendEvent } = require('../services/kafkaProducer');

exports.receiveEvent = async (req, res) => {
    try {
        const eventData = req.body;

        // Validation basique
        if (!eventData || !eventData.eventType) {
            return res.status(400).json({ message: "Données d'événement invalides" });
        }

        // Envoi asynchrone à Kafka
        sendEvent(eventData);

        // Réponse immédiate au client 
        res.status(200).json({ message: "Événement tracké avec succès" });

    } catch (error) {
        console.error(" Erreur dans le contrôleur de tracking:", error);
        res.status(500).json({ message: "Erreur interne du serveur" });
    }
};