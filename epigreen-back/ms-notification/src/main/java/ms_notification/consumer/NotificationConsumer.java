package ms_notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import ms_notification.dto.NotificationMessage;
import ms_notification.service.GmailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NotificationConsumer {

    @Autowired
    private GmailService gmailService;

    // L'ObjectMapper doit être instancié une seule fois
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Récupère la liste depuis application.properties
    @Value("${app.notification.whitelist}")
    private List<String> whitelist;

    @KafkaListener(topics = "order-notifications", groupId = "notification-group")
    public void listen(String message) {
        try {
            // 1. Désérialisation du JSON avec l'ObjectMapper
            NotificationMessage notification = objectMapper.readValue(message, NotificationMessage.class);

            String email = notification.getCustomerEmail();

            // 2. Vérification de la Whitelist
            if (whitelist != null && whitelist.contains(email)) {
                System.out.println("Email autorisé (Whitelist) : " + email);
                gmailService.sendStatusEmail(notification);
            } else {
                System.out.println("Email ignoré (Hors Whitelist) : " + email);
            }

        } catch (Exception e) {
            System.err.println("Erreur de traitement de la notification : " + e.getMessage());
        }
    }
}