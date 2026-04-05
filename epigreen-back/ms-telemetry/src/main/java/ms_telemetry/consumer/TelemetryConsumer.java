package ms_telemetry.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class TelemetryConsumer {

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(TelemetryConsumer.class);

    @Value("${api.delivery.url:http://localhost:8087/api/delivery}")
    private String deliveryApiUrl;

    // LE SECRET EST ICI : En changeant le nom du groupe (v2), on force Kafka à tout relire depuis le début !
    @KafkaListener(topics = "gps-data-raw", groupId = "telemetry-group-v2")
    public void consume(String message) {
        try {
            JsonNode node = mapper.readTree(message);
            long deliveryId = node.get("deliveryId").asLong();
            double lat = node.get("lat").asDouble();
            double lon = node.get("lon").asDouble();

            String url = deliveryApiUrl + "/" + deliveryId + "/location?lat=" + lat + "&lon=" + lon;
            
            // Appel HTTP
            restTemplate.put(url, null);

            // Log explicite pour la console
            log.info("✅ UPDATE ENVOYÉE À L'API : Livreur {} -> {},{}", deliveryId, lat, lon);

        } catch (Exception e) {
            // On log l'erreur proprement
            log.error("❌ ERREUR API DELIVERY (Message: {}) : {}", message, e.getMessage());
        }
    }
}
