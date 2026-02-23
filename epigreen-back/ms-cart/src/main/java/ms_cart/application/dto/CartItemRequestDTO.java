package ms_cart.application.dto;

import lombok.Data;

@Data
public class CartItemRequestDTO {
    private Long productId;
    private String productName;
    private Double price;
    private Integer quantity;
}