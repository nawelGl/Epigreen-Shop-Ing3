package ms_notification.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ms_notification.dto.Notification;
import ms_notification.websocket.NotificationWebSocketHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RedisReceiver {

    @Autowired
    private NotificationWebSocketHandler webSocketHandler;

    @Autowired
    private ObjectMapper objectMapper;

    public void receiveMessage(String message) {
        log.info("[REDIS -> IN] Message reçu du cluster : {}", message);
        try {
            // On lit le message comme une Notification (l'entité sauvée en base)
            Notification notification = objectMapper.readValue(message, Notification.class);
            String userId = notification.getUserId();

            if (userId != null) {
                // On transmet l'objet entier au WebSocket
                webSocketHandler.sendNotification(userId, notification);
            }
        } catch (Exception e) {
            log.error("Erreur de désérialisation Redis : {}. Message brut : {}", e.getMessage(), message);
        }
    }
}