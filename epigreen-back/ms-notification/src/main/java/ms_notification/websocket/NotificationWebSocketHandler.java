package ms_notification.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import ms_notification.dto.NotificationMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    public void sendNotification(String userId, NotificationMessage message) {
        WebSocketSession session = activeSessions.get(userId);

        try {
            String jsonMessage = objectMapper.writeValueAsString(message);

            if (session != null && session.isOpen()) {
                // CAS 1 : CHEMIN NOMINAL (RAM)
                session.sendMessage(new TextMessage(jsonMessage));
                System.out.println("[ENVOI] Notification envoyée directement via RAM à l'User " + userId);

            } else {
                // CAS 2 : LE FALLBACK (Crash) - On interroge Redis
                String targetInstance = redisTemplate.opsForValue().get("ws_user_" + userId);

                if (targetInstance != null) {
                    System.out.println("[FALLBACK] L'User " + userId + " n'est pas ici. Redis dit qu'il est sur : "
                            + targetInstance);
                    // TODO: Faire un appel HTTP (RestTemplate/WebClient) vers 'targetInstance'
                    // pour lui transférer le message.
                    // (Pour la démo immédiate, on log juste l'action)
                } else {
                    System.out.println("[IGNORÉ] L'User " + userId
                            + " n'est connecté nulle part. Notification sauvegardée uniquement en BDD.");
                }
            }
        } catch (Exception e) {
            System.err.println("Erreur d'envoi WebSocket : " + e.getMessage());
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