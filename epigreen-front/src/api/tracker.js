import axios from 'axios';
import { CONFIG } from './config';

// Générateur d'ID unique pour l'événement
const generateUUID = () => {
    if (typeof crypto !== 'undefined' && crypto.randomUUID) {
        return crypto.randomUUID();
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
};

export const trackEvent = (eventType, eventData = {}) => {
    const userId = localStorage.getItem('epigreen_user_id');
    
    // payload Kafka
    const payload = {
        eventId: generateUUID(),
        eventType: eventType,
        userId: userId,
        eventData: eventData,
        ts: new Date().toISOString(),
        metadata: { 
            device: "web" 
        }
    };

    // envoyer à ms-event-tracker
    axios.post(CONFIG.API.EVENTTRACKER, payload)
        .then(() => console.log(`[EventTracker] Événement ${eventType} envoyé `))
        .catch(err => console.error(`[EventTracker] Erreur :`, err));
};