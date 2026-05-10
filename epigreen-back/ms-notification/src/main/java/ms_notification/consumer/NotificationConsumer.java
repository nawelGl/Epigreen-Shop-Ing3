package ms_notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import ms_notification.dto.Notification;
import ms_notification.dto.NotificationMessage;
import ms_notification.repository.NotificationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;

@Slf4j
@Service
public class NotificationConsumer {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private NotificationRepository notificationRepository;

    @KafkaListener(topics = "delivery-events", groupId = "notification-group")
    public void listen(String message) {
        log.info("[KAFKA -> IN] Instance Notification a récupéré l'événement : {}", message);

        try {
            // 1. Désérialisation via le DTO (Le Messager)
            NotificationMessage dto = objectMapper.readValue(message, NotificationMessage.class);

            // Vérification que l'ID est bien présent
            String customerId = dto.getCustomerId();
            if (customerId == null || customerId.isEmpty()) {
                log.error("Erreur : Impossible de router la notification, customerId manquant.");
                return;
            }

            if (dto.getStatus().equals("DELIVERED")) {
                log.info("[TRAITEMENT] Statut DELIVERED détecté pour l'User {}", customerId);
            }

            // 2. Création et SAUVEGARDE EN BASE de l'Entité (Le filet de sécurité)
            Notification entity = new Notification();
            entity.setUserId(customerId);

            // Création du texte lisible pour l'utilisateur
            String texteNotification = "Mise à jour : Votre commande " + dto.getTrackingNumber()
                    + " est maintenant au statut : " + dto.getStatus();
            entity.setMessage(texteNotification);

            entity.setCreatedAt(LocalDateTime.now());
            entity.setRead(false); // Par défaut, non lue

            // On enregistre en base de données PostgreSQL
            notificationRepository.save(entity);
            log.info("[BASE DE DONNÉES] Notification sauvegardée avec succès.");

            // 3. TRANSFERT DANS REDIS (Pour le temps réel)
            // On convertit l'entité (qui possède maintenant un ID de base de données) en
            // JSON
            String jsonEntity = objectMapper.writeValueAsString(entity);

            log.info("[REDIS -> OUT] Diffusion de l'événement au cluster pour envoi WebSocket...");
            redisTemplate.convertAndSend("notifications", jsonEntity);

        } catch (Exception e) {
            log.error("Erreur de traitement Kafka : {}", e.getMessage());
        }
    }
}