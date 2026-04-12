package ms_notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import ms_notification.dto.NotificationMessage;
import ms_notification.websocket.NotificationWebSocketHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

    @Autowired
    private NotificationWebSocketHandler webSocketHandler;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "delivery-events", groupId = "notification-group")
    public void listen(String message) {
        try {
            // 1. Désérialisation
            NotificationMessage notification = objectMapper.readValue(message, NotificationMessage.class);

            // 2. Vérification que l'ID est bien présent
            String customerId = notification.getCustomerId();
            if (customerId == null || customerId.isEmpty()) {
                System.err.println("Erreur : Impossible de router la notification, customerId manquant.");
                return;
            }

            System.out.println("Message Kafka reçu pour l'User " + customerId);

            // 3. Transfert
            webSocketHandler.sendNotification(customerId, notification);

        } catch (Exception e) {
            System.err.println("Erreur de traitement Kafka : " + e.getMessage());
        }
    }
}