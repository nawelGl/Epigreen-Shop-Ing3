package main.java.fr.upec.episen.sirius.epigreen.ms_cart.application.dto;


import lombok.Data;

@Data
public class CartItemRequestDTO {
    private Long productId;
    private String productName;
    private Double price;
    private Interger quantity;
}
