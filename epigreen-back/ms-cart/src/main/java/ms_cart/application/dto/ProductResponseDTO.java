package ms_cart.application.dto;

import lombok.Data;

@Data
public class ProductResponseDTO {
    private Integer id;
    private String name;
    private Double price;
}