package ms_notification.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;
import ms_notification.dto.Notification;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.web.socket.WebSocketSession;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class NotificationWebSocketHandler extends TextWebSocketHandler {

    // 1. RAM (Thread-safe)
    private final Map<String, WebSocketSession> activeSessions = new ConcurrentHashMap<>();

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    // Récupère le nom de l'instance défini dans Docker
    @Value("${INSTANCE_NAME:default_instance}")
    private String instanceName;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        // Extraction du userId depuis l'URL (ex:
        // ws://localhost:8000/notifications?userId=123)
        String userId = getUserIdFromSession(session);

        if (userId != null) {
            // A. Stockage en RAM
            activeSessions.put(userId, session);
            System.out.println("[CONNEXION] User " + userId + " connecté sur " + instanceName);

            // B. Mise a jour de l'Annuaire Redis (écrase toute ancienne valeur)
            redisTemplate.opsForValue().set("ws_user_" + userId, instanceName);
        } else {
            session.close(CloseStatus.BAD_DATA);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        String userId = getUserIdFromSession(session);
        if (userId != null) {
            // A. Nettoyage de la RAM
            activeSessions.remove(userId);
            System.out.println("[DÉCONNEXION] User " + userId + " déconnecté de " + instanceName);

            // B. Nettoyage propre de Redis
            redisTemplate.delete("ws_user_" + userId);
        }
    }

    // --- LA MÉTHODE APPELÉE PAR KAFKA ---
    public void sendNotification(String userId, Notification notification) {
        // 1. On cherche la session correspondant à l'utilisateur dans notre Map locale
        WebSocketSession session = activeSessions.get(userId);

        // 2. Si la session existe et qu'elle est toujours ouverte sur CETTE instance
        if (session != null && session.isOpen()) {
            try {
                // 3. On transforme l'objet Notification complet en chaîne JSON
                // C'est ce JSON qui contiendra les champs 'id', 'message', 'createdAt', etc.
                String jsonPayload = objectMapper.writeValueAsString(notification);

                // 4. On envoie le message à travers le tuyau WebSocket
                session.sendMessage(new TextMessage(jsonPayload));

                log.info("[WEBSOCKET -> OUT] Message envoyé avec succès à l'utilisateur {}", userId);
            } catch (Exception e) {
                log.error("Erreur lors de la sérialisation ou de l'envoi WS : {}", e.getMessage());
            }
        } else {
            log.debug("[IGNORED] L'utilisateur {} n'est pas connecté sur cette instance (Session introuvable).",
                    userId);
        }
    }

    private String getUserIdFromSession(WebSocketSession session) {
        URI uri = session.getUri();
        if (uri != null && uri.getQuery() != null) {
            return UriComponentsBuilder.fromUri(uri).build().getQueryParams().getFirst("userId");
        }
        return null;
    }
}