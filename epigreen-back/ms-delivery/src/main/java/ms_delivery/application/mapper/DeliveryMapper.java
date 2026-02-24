package ms_delivery.application.mapper;

import org.springframework.stereotype.Component;

import ms_delivery.application.dto.DeliveryResponseDTO;
import ms_delivery.domain.entity.Delivery;

@Component
public class DeliveryMapper {

    public DeliveryResponseDTO toResponseDTO(Delivery delivery) {
        if (delivery == null) {
            return null;
        }

        return DeliveryResponseDTO.builder()
                .id(delivery.getId())
                .trackingNumber(delivery.getTrackingNumber())
                .status(delivery.getStatus())
                .deliveryMethod(delivery.getDeliveryMethod())
                .destinationCity(delivery.getDestCity())
                .destinationStreet(delivery.getDestStreet())
                .destinationZipCode(delivery.getDestZipCode())
                .currentLat(delivery.getCurrentLat())
                .currentLon(delivery.getCurrentLon())
                .distanceKm(delivery.getCalculatedDistance())
                .carbonFootprint(delivery.getCarbonFootprint())
                .score(delivery.getDeliveryScore())
                .estimatedDeliveryDate(delivery.getEstimatedDeliveryDate())
                .originLat(delivery.getOriginLat())
                .originLon(delivery.getOriginLon())
                .destLat(delivery.getDestLat())
                .destLon(delivery.getDestLon())
                .build();
    }
}