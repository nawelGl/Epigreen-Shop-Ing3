package ms_delivery.application.dto;

import lombok.Data;
import ms_delivery.domain.entity.DeliveryMethod;

@Data
public class DeliveryCheckoutDTO {
    private Long deliveryId;
    private DeliveryMethod deliveryMethod;
}
