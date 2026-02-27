package ms_delivery.application.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.kafka.core.KafkaTemplate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ms_delivery.application.dto.DeliveryCreateDTO;
import ms_delivery.application.dto.GeoapifyResponseDTO;
import ms_delivery.application.service.geoapify.GeoapifyService;
import ms_delivery.domain.entity.Delivery;
import ms_delivery.domain.entity.DeliveryMethod;
import ms_delivery.domain.entity.DeliveryScore;
import ms_delivery.domain.entity.DeliveryStatus;
import ms_delivery.domain.repository.DeliveryRepository;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class DeliveryService {

    @Autowired
    private DeliveryRepository deliveryRepository;
    @Autowired
    private GeoapifyService geoapifyService;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    public Delivery findById(Long id) {
        return deliveryRepository.findById(id).orElse(null);
    }

    public List<Delivery> findAll() {
        return deliveryRepository.findAll();
    }

    /**
     * Calcule la distance, le CO2, et le score en une seule transaction.
     */
    public Delivery finalizeDeliverySetup(Delivery deliveryInfo) {
        Optional<Delivery> optDelivery = deliveryRepository.findById(deliveryInfo.getId());
        if (!optDelivery.isPresent())
            return null;

        Delivery delivery = optDelivery.get();
        delivery.setDeliveryMethod(deliveryInfo.getDeliveryMethod());
        delivery.setStatus(DeliveryStatus.PENDING);

        // 1. Calcul de la distance
        Double distance = calculateDistance(delivery.getOriginLat(), delivery.getOriginLon(),
                delivery.getDestLat(), delivery.getDestLon());
        delivery.setCalculatedDistance(distance);
        log.info("Distance calculée : " + distance + " km.");

        // 2. Calcul du CO2
        double factor = (delivery.getDeliveryMethod() == DeliveryMethod.POINT_RELAIS) ? 0.08 : 0.15;
        Double co2 = Math.round((distance * factor) * 100.0) / 100.0;
        delivery.setCarbonFootprint(co2);
        log.info("CO2 calculé : " + co2 + " kg.");

        // 3. Attribution du Score
        delivery.setDeliveryScore(calculateScore(co2));
        log.info("Score de livraison calculé : " + calculateScore(co2) + ".");

        return deliveryRepository.save(delivery);
    }

    /**
     * Méthode pour le futur simulateur IoT
     */
    public Delivery updateCurrentLocation(Long deliveryId, Double lat, Double lon) {
        Optional<Delivery> opt = deliveryRepository.findById(deliveryId);
        if (opt.isPresent()) {
            Delivery delivery = opt.get();
            delivery.setCurrentLat(lat);
            delivery.setCurrentLon(lon);

            if (delivery.getStatus() == DeliveryStatus.PENDING) {
                delivery.setStatus(DeliveryStatus.IN_TRANSIT);
            }
            return deliveryRepository.save(delivery);
        }
        return null;
    }


    private Double calculateDistance(Double lat1, Double lon1, Double lat2, Double lon2) {
        if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
            return null;
        }
        final double R = 6371.0; // km
        double latDist = Math.toRadians(lat2 - lat1);
        double lonDist = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDist / 2) * Math.sin(latDist / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(lonDist / 2) * Math.sin(lonDist / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    private DeliveryScore calculateScore(Double carbonFootprint) {
        if (carbonFootprint == null || carbonFootprint < 0)
            return DeliveryScore.E;
        if (carbonFootprint <= 35)
            return DeliveryScore.A;
        if (carbonFootprint <= 70)
            return DeliveryScore.B;
        if (carbonFootprint <= 105)
            return DeliveryScore.C;
        if (carbonFootprint <= 140)
            return DeliveryScore.D;
        return DeliveryScore.E;
    }

    /**
     * Création initiale de la livraison (généralement appelée par ms-order)
     */
    public Delivery createDelivery(DeliveryCreateDTO createDTO) {
        Delivery newDelivery = Delivery.builder()
                .orderId(createDTO.getOrderId())
                .customerId(createDTO.getCustomerId())
                .originWarehouseId(createDTO.getOriginWarehouseId())
                .originLat(createDTO.getOriginLat())
                .originLon(createDTO.getOriginLon())
                .destStreet(createDTO.getDestStreet())
                .destCity(createDTO.getDestCity())
                .destZipCode(createDTO.getDestZipCode())
                .destLat(createDTO.getDestLat())
                .destLon(createDTO.getDestLon())
                .status(DeliveryStatus.PENDING)
                // On génère un numéro de suivi temporaire ou définitif dès la création
                .trackingNumber("TRK-" + java.util.UUID.randomUUID().toString().substring(0, 8).toUpperCase())
                .build();

                if (newDelivery.getDestLat() == null || newDelivery.getDestLon() == null) {

                if (newDelivery.getDestStreet() == null || newDelivery.getDestCity() == null
                        || newDelivery.getDestZipCode() == null) {
                    throw new IllegalArgumentException("Adresse de destination incomplète (street/city/zipCode requis).");
                }

                String fullAddress = String.format("%s, %s %s, France",
                        newDelivery.getDestStreet(),
                        newDelivery.getDestZipCode(),
                        newDelivery.getDestCity());

                String normalized = fullAddress.replaceAll("\\s+", " ").trim();

                GeoapifyResponseDTO geo = geoapifyService.getCoordinatesFromAddress(normalized);

                if (geo == null) {
                    throw new IllegalStateException("Geoapify: aucune coordonnée trouvée pour: " + fullAddress);
                }

                newDelivery.setDestLat(geo.getLatitude());
                newDelivery.setDestLon(geo.getLongitude());
            }

            log.info("Livraison " + newDelivery.getTrackingNumber() + " créée et enregistrée en base de données !");

        return deliveryRepository.save(newDelivery);
    }

    public List<Delivery> findByCustomerId(Long customerId) {
        return deliveryRepository.findByCustomerId(customerId);
    }


    public Delivery updateStatus(Long id, DeliveryStatus status) {
        Delivery delivery = deliveryRepository.findById(id).orElse(null);
        if (delivery != null) {
            delivery.setStatus(status);
            Delivery savedDelivery = deliveryRepository.save(delivery);

            // Déclenchement Kafka si le statut passe manuellement à DELIVERED
            if (status == DeliveryStatus.DELIVERED) {
                log.info("La livraison " + delivery.getTrackingNumber() + " est passée en statut DELIVERED. Événement produit dans Kafka (topic : order-notifications) !");
                sendNotification(savedDelivery);
            }

            return savedDelivery;
        }
        return null;
    }

    public void updateCurrentLocation(String jsonPayload) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(jsonPayload);
            Long deliveryId = node.get("deliveryId").asLong();
            Double lat = node.get("lat").asDouble();
            Double lon = node.get("lon").asDouble();

            Delivery delivery = deliveryRepository.findById(deliveryId).orElse(null);
            if (delivery != null) {
                delivery.setCurrentLat(lat);
                delivery.setCurrentLon(lon);

                boolean justDelivered = false;

                if (delivery.getStatus() != DeliveryStatus.DELIVERED) {
                    delivery.setStatus(DeliveryStatus.IN_TRANSIT);
                }

                // Vérification de la distance (200m)
                Double distKm = calculateDistance(lat, lon, delivery.getDestLat(), delivery.getDestLon());
                if (distKm != null && distKm <= 0.2) {
                    if (delivery.getStatus() != DeliveryStatus.DELIVERED) {
                        delivery.setStatus(DeliveryStatus.DELIVERED);
                        justDelivered = true;
                    }
                }

                deliveryRepository.save(delivery);

                // Déclenchement Kafka uniquement à l'instant où on arrive à destination
                if (justDelivered) {
                    sendNotification(delivery);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendNotification(Delivery delivery) {
        try {
            // MAIL FORCÉ POUR LES TESTS
            String targetEmail = "ghazal.nawel@gmail.com";

            String jsonMessage = String.format(
                    "{\"customerEmail\":\"%s\", \"customerName\":\"Client #%d\", \"status\":\"DELIVERED\", \"trackingNumber\":\"%s\"}",
                    targetEmail,
                    delivery.getCustomerId() != null ? delivery.getCustomerId() : 0,
                    delivery.getTrackingNumber() != null ? delivery.getTrackingNumber() : "INCONNU");

            kafkaTemplate.send("order-notifications", jsonMessage).get();
            log.info("Message JSON produit dans Kafka : " + jsonMessage);
        } catch (Exception e) {
            System.err.println("Erreur lors de l'envoi Kafka : " + e.getMessage());
        }
    }

}