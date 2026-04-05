package ms_telemetry;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class MqttToKafkaBridge {

    private static final Logger log = LoggerFactory.getLogger(MqttToKafkaBridge.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    // L'injection par constructeur garantit que KafkaTemplate est 100% prêt
    public MqttToKafkaBridge(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${mqtt.broker.url:tcp://localhost:1883}") String brokerUrl) throws MqttException {
        
        this.kafkaTemplate = kafkaTemplate;
        String publisherId = UUID.randomUUID().toString();
        IMqttClient client = new MqttClient(brokerUrl, publisherId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);

        log.info("📡 Tentative de connexion MQTT sur : {}", brokerUrl);
        client.connect(options);
        log.info("✅ Connecté à MQTT ! En écoute sur delivery/location...");

// On écoute MQTT
        client.subscribe("delivery/location", (topic, msg) -> {
            String payload = new String(msg.getPayload());
            log.info("📥 [MESSAGE REÇU MQTT] : {}", payload);
            
            // BONNE PRATIQUE : Envoi asynchrone avec gestion des erreurs
            this.kafkaTemplate.send("gps-data-raw", payload).whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("✅ [CONFIRMÉ PAR KAFKA] : Donnée enregistrée avec succès");
                } else {
                    log.error("❌ [ERREUR FATALE KAFKA] : {}", ex.getMessage());
                }
            });
        });
    }
}
