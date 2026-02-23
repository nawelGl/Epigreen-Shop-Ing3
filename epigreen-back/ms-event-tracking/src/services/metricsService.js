
const client = require('prom-client');

// Créer un registre 
const register = new client.Registry();

// Ajouter les métriques par défaut(CPU, RAM).
client.collectDefaultMetrics({ register });

// Création compteur métier personnalisé => Count de évènement tracé
const trackCounter = new client.Counter({
    name: 'user_tracking_events_total',
    help: 'Total des événements de tracking envoyés à Kafka',
    labelNames: ['event_type'] 
});


register.registerMetric(trackCounter);

// Exporter le registre et le compteur 
module.exports = {
    register,
    trackCounter
};