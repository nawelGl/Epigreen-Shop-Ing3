package ms_order.application.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class OrderRequestDTO {
    @NotNull
    private Integer customerId;
    
    @NotBlank
    private String shippingStreet;
    
    @NotBlank
    private String shippingCity;
    
    @NotBlank
    private String shippingZipCode;
    
    private String shippingCountry = "France"; // commande en Franece par défaut
}