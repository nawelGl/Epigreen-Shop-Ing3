package ms_order.application.dto;

import lombok.Data;
import java.math.BigDecimal;

@Data
public class CartItemDTO {
    private Integer productId;
    private String productName;
    private BigDecimal price;
    private Integer quantity;
    private String sizeLabel;
}