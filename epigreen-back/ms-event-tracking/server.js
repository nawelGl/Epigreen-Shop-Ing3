const express = require('express');
const cors = require('cors');
require('dotenv').config();

const trackRoutes = require('./src/routes/trackRoutes');
const { connectProducer } = require('./src/services/kafkaProducer');
const { register } = require('./src/services/metricsService');

const app = express();
const PORT = process.env.PORT || 3000;

// Middlewares
app.use(cors()); 
app.use(express.json()); 

// Routes
app.use('/api/track', trackRoutes);

// Démarrage du serveur et connexion à Kafka
app.listen(PORT, async () => {
    console.log(` ===== Service ms-usertracking démarré sur http://localhost:${PORT} ======`);
    
    // Initialisation de la connexion Kafka
    await connectProducer();
});





// route pour Promethus
app.get('/metrics', async (req, res) => {
    try {
        res.set('Content-Type', register.contentType);
        res.end(await register.metrics());
    } catch (ex) {
        res.status(500).end(ex);
    }
});


app.listen(PORT, () => {
    console.log(`Tracking service running on port ${PORT}`);
    console.log(`Metrics available at http://localhost:${PORT}/metrics`);
});