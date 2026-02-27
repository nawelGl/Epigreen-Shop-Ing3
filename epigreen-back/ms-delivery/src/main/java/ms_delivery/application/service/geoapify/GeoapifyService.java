package ms_delivery.application.service.geoapify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ms_delivery.application.dto.GeoapifyResponseDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.springframework.http.HttpHeaders;

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
            URI uri = UriComponentsBuilder
                    .fromHttpUrl("https://api.geoapify.com/v1/geocode/search")
                    .queryParam("text", address)
                    .queryParam("format", "json")
                    .queryParam("limit", 1)
                    .queryParam("filter", "countrycode:fr")
                    .queryParam("apiKey", apiKey)
                    .build()
                    .encode()
                    .toUri();

            HttpHeaders headers = new HttpHeaders();
            headers.set("User-Agent", "Epigreen-ms-delivery/1.0");
            headers.set("Accept", "application/json");

            ResponseEntity<String> responseEntity =
                    restTemplate.exchange(uri, HttpMethod.GET, new HttpEntity<>(headers), String.class);

            String response = responseEntity.getBody();
            System.out.println("Geoapify raw response = " + response);

            JsonNode root = objectMapper.readTree(response);
            JsonNode results = root.path("results");

            if (results.isArray() && results.size() > 0) {
                JsonNode r = results.get(0);

                GeoapifyResponseDTO dto = new GeoapifyResponseDTO();
                dto.setLatitude(r.path("lat").asDouble());
                dto.setLongitude(r.path("lon").asDouble());
                dto.setFormattedAddress(r.path("formatted").asText(null));
                dto.setCountry(r.path("country").asText(null));
                return dto;
            }

            throw new RuntimeException("Aucune coordonnée trouvée pour : " + address);

        } catch (Exception e) {
            throw new RuntimeException("Erreur Geoapify : " + e.getMessage(), e);
        }
    }
}
