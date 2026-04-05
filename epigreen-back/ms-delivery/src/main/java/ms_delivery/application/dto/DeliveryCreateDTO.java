package ms_delivery.application.dto;

import lombok.Data;

@Data
public class DeliveryCreateDTO {
    private Long orderId;
    private Long customerId;
    private Long originWarehouseId;
    private Double originLat;
    private Double originLon;
    private String destStreet;
    private String destCity;
    private String destZipCode;
    private Double destLat;
    private Double destLon;
}