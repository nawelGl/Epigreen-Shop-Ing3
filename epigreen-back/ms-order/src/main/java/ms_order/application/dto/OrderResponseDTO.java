package ms_order.application.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class OrderResponseDTO {
    private Integer id;
    private Integer customerId;
    private String status;
    private BigDecimal totalPrice;
    private String shippingStreet;
    private String shippingCity;
    private String shippingZipCode;
    private String shippingCountry;
    private LocalDateTime createdAt;

}