package ms_product.application.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockResponseDTO {
    private Integer id;
    private Integer productId;
    private String sizeLabel;
    private Integer quantity;
    private Integer warehouseId;
}