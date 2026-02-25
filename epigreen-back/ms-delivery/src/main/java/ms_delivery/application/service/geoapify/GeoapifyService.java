package ms_delivery.application.service.geoapify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import ms_delivery.application.dto.GeoapifyResponseDTO;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;
import java.util.List;

@Service
public class GeoapifyService {

    @Value("${geoapify.key}")
    private String apiKey;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Autocomplétion : Renvoie une liste d'adresses suggérées
     */
    public List<String> autocompleteAddress(String userInput) {
        List<String> suggestions = new ArrayList<>();
        try {
            // 1. On construit l'URL proprement (SANS espaces manuels dans Bruno !)
            String url = UriComponentsBuilder.fromHttpUrl("https://api.geoapify.com/v1/geocode/autocomplete")
                    .queryParam("text", userInput)
                    .queryParam("format", "json")
                    .queryParam("lang", "fr")
                    .queryParam("filter", "countrycode:fr")
                    .queryParam("apiKey", apiKey)
                    .toUriString();

            // 2. On récupère la réponse brute
            String response = restTemplate.getForObject(url, String.class);

            // 3. On parse avec précaution
            JsonNode root = objectMapper.readTree(response);
            JsonNode results = root.get("results");

            if (results != null && results.isArray()) {
                for (JsonNode node : results) {
                    String formatted = node.path("formatted").asText();
                    if (!formatted.isEmpty()) {
                        suggestions.add(formatted);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return suggestions;
    }


    /**
     * Géocodage : Transforme une adresse texte en coordonnées GPS (Lat/Lon)
     */
    public GeoapifyResponseDTO getCoordinatesFromAddress(String address) {
        try {
            String url = UriComponentsBuilder.fromHttpUrl("https://api.geoapify.com/v1/geocode/search")
                    .queryParam("text", address)
                    .queryParam("apiKey", apiKey)
                    .toUriString();

            String response = restTemplate.getForObject(url, String.class);
            JsonNode root = objectMapper.readTree(response);
            JsonNode features = root.path("features");

            if (features.isArray() && features.size() > 0) {
                JsonNode properties = features.get(0).path("properties");
                return objectMapper.treeToValue(properties, GeoapifyResponseDTO.class);
            }
        } catch (Exception e) {
            System.err.println("Erreur Geoapify Geocoding : " + e.getMessage());
        }
        return null;
    }
}
