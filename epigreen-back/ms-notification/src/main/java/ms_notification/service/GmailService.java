package ms_notification.service;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.Message;
import ms_notification.dto.NotificationMessage;
import org.springframework.stereotype.Service;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Properties;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class GmailService {

    private static final String APPLICATION_NAME = "ms-notification";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";
    private static final Logger log = LoggerFactory.getLogger(GmailService.class);

    // Définit qu'on veut le droit d'envoyer des mails
    private static final java.util.List<String> SCOPES = Collections.singletonList(GmailScopes.GMAIL_SEND);

    /**
     * Méthode d'authentification OAuth 2.0 (Ouvre le navigateur)
     */
    private Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws Exception {
        InputStream in = GmailService.class.getResourceAsStream("/credentials.json");
        if (in == null) {
            throw new RuntimeException("Fichier credentials.json introuvable !");
        }

        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Configure le flux d'autorisation
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();

        // Démarre un serveur local sur le port 8888 pour écouter le retour de Google
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    /**
     * Méthode principale appelée par le Consumer Kafka
     */
    public void sendStatusEmail(NotificationMessage notification) {
        try {
            final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

            // 1. Authentification
            Credential credential = getCredentials(HTTP_TRANSPORT);

            // 2. Initialisation du client Gmail
            Gmail service = new Gmail.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                    .setApplicationName(APPLICATION_NAME)
                    .build();

            // 3. Création du contenu de l'email
            Properties props = new Properties();
            Session session = Session.getDefaultInstance(props, null);
            MimeMessage email = new MimeMessage(session);

            email.setFrom(new InternetAddress("me")); 
            email.addRecipient(jakarta.mail.Message.RecipientType.TO,
                    new InternetAddress(notification.getCustomerEmail()));
            email.setSubject("EpiGreen Shop -  Mise à jour de votre livraison : " + notification.getTrackingNumber());
            email.setText("Bonjour " + notification.getCustomerName() + ",\n\nVotre commande chez EpiGreen Shop est maintenant : "
                    + notification.getStatus() + ".\n\nMerci pour votre commande !\nL'équipe EpiGreen-Shop");

            // 4. Encodage et Envoi
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            email.writeTo(buffer);
            byte[] rawMessageBytes = buffer.toByteArray();
            String encodedEmail = Base64.getUrlEncoder().encodeToString(rawMessageBytes);

            Message message = new Message();
            message.setRaw(encodedEmail);

            service.users().messages().send("me", message).execute();
            log.info("Email envoyé via OAuth2 avec succès à : " + notification.getCustomerEmail());

        } catch (Exception e) {
            System.err.println("Erreur OAuth2/Gmail : " + e.getMessage());
            e.printStackTrace();
        }
    }
}