package ms_delivery.application.dto;

import lombok.Builder;
import lombok.Data;
import ms_delivery.domain.entity.DeliveryMethod;
import ms_delivery.domain.entity.DeliveryScore;
import ms_delivery.domain.entity.DeliveryStatus;

import java.time.LocalDateTime;

@Data
@Builder
public class DeliveryResponseDTO {
    private Long id;
    private String trackingNumber;
    private DeliveryStatus status;
    private DeliveryMethod deliveryMethod;
    private String destinationStreet;
    private String destinationCity;
    private String destinationZipCode;
    private Double currentLat;
    private Double currentLon;
    private Double distanceKm;
    private Double carbonFootprint;
    private DeliveryScore score;
    private LocalDateTime estimatedDeliveryDate;
    private Double calculatedDistance;
    private String deliveryScore;
}