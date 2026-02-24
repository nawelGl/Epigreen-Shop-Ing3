package ms_delivery.application.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ms_delivery.application.dto.DeliveryCreateDTO;
import ms_delivery.domain.entity.Delivery;
import ms_delivery.domain.entity.DeliveryMethod;
import ms_delivery.domain.entity.DeliveryScore;
import ms_delivery.domain.entity.DeliveryStatus;
import ms_delivery.domain.repository.DeliveryRepository;
import java.util.List;
import java.util.Optional;

@Service
public class DeliveryService {

    @Autowired
    private DeliveryRepository deliveryRepository;

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

        // 2. Calcul du CO2
        double factor = (delivery.getDeliveryMethod() == DeliveryMethod.POINT_RELAIS) ? 0.08 : 0.15;
        Double co2 = Math.round((distance * factor) * 100.0) / 100.0;
        delivery.setCarbonFootprint(co2);

        // 3. Attribution du Score
        delivery.setDeliveryScore(calculateScore(co2));

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
        if (lat1 == null || lon1 == null || lat2 == null || lon2 == null)
            return 0.0;
        final int R = 6371; // Rayon de la terre
        double latDist = Math.toRadians(lat2 - lat1);
        double lonDist = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDist / 2) * Math.sin(latDist / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(lonDist / 2) * Math.sin(lonDist / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return Math.round((R * c) * 100.0) / 100.0;
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

        return deliveryRepository.save(newDelivery);
    }

    public Delivery updateStatus(Long id, DeliveryStatus status) {
        Optional<Delivery> opt = deliveryRepository.findById(id);
        if (opt.isPresent()) {
            Delivery delivery = opt.get();
            delivery.setStatus(status);

            if (status == DeliveryStatus.DELIVERED) {
                System.out.println("Déclenchement de l'envoi du mail à l'utilisateur...");
                // TODO : dev service de mail
            }

            return deliveryRepository.save(delivery);
        }
        return null;
    }

    public List<Delivery> findByCustomerId(Long customerId) {
        return deliveryRepository.findByCustomerId(customerId);
    }
}