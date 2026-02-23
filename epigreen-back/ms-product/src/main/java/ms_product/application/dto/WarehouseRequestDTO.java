package ms_product.application.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WarehouseRequestDTO {

    @NotBlank(message = "Le nom est obligatoire")
    @Size(max = 100)
    private String name;

    @Size(max = 100)
    private String city;

    @Size(max = 255)
    private String street;

    @Size(max = 20)
    private String zipCode;

    @Size(max = 100)
    private String country;

    private BigDecimal gpsLat;
    private BigDecimal gpsLong;
}