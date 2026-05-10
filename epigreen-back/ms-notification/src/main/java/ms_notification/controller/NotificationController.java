package ms_notification.controller;

import ms_notification.dto.Notification;
import ms_notification.repository.NotificationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/notifications")
@CrossOrigin(origins = "*")
public class NotificationController {

    @Autowired
    private NotificationRepository notificationRepository;

    /**
     * ÉTAPE 1 : Le Rattrapage
     * Appelé par React dès que l'utilisateur arrive sur la page.
     */
    @GetMapping("/unread/{userId}")
    public ResponseEntity<List<Notification>> getUnreadNotifications(@PathVariable String userId) {
        // On récupère toutes les notifications "non lues" en base
        List<Notification> unread = notificationRepository.findByUserIdAndIsReadFalse(userId);
        return ResponseEntity.ok(unread);
    }

    /**
     * ÉTAPE 2 : Le Nettoyage
     * Appelé par React une fois que l'utilisateur a vu les notifications.
     */
    @PostMapping("/mark-as-read/{userId}")
    public ResponseEntity<Void> markAsRead(@PathVariable String userId) {
        List<Notification> unread = notificationRepository.findByUserIdAndIsReadFalse(userId);
        unread.forEach(n -> n.setRead(true));
        notificationRepository.saveAll(unread);
        return ResponseEntity.ok().build();
    }
}