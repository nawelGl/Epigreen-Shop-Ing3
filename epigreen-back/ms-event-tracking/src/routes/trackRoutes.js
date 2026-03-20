const express = require('express');
const router = express.Router();
const trackController = require('../controllers/trackController');

// Route POST pour recevoir les événements
// URL finale : http://localhost:3000/api/track/events
router.post('/events', trackController.receiveEvent);

module.exports = router;