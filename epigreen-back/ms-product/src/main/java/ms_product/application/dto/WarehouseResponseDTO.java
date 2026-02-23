package ms_product.application.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WarehouseResponseDTO {
    private Integer id;
    private String name;
    private String city;
    private String street;
    private String zipCode;
    private String country;
    private BigDecimal gpsLat;
    private BigDecimal gpsLong;
}