package ms_telemetry.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class TelemetryConsumer {

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(topics = "gps-data-raw", groupId = "telemetry-group")
    public void consume(String message) {
        try {
            JsonNode node = mapper.readTree(message);
            long deliveryId = node.get("deliveryId").asLong();
            double lat = node.get("lat").asDouble();
            double lon = node.get("lon").asDouble();

            String url = "http://localhost:8087/api/delivery/" + deliveryId + "/location?lat=" + lat + "&lon=" + lon;
            restTemplate.put(url, null);

            System.out.println("Update envoyée à ms-delivery : " + deliveryId + " -> " + lat + "," + lon);
        } catch (Exception e) {
            System.err.println("Erreur parsing/push telemetry: " + message);
            e.printStackTrace();
        }
    }
}