package ms_product.application.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockRequestDTO {

    @NotNull(message = "L'ID du produit est obligatoire")
    private Integer productId;

    @NotBlank(message = "La taille est obligatoire")
    @Size(max = 10)
    private String sizeLabel;

    @NotNull(message = "La quantité est obligatoire")
    @Min(value = 0, message = "La quantité ne peut pas être négative")
    private Integer quantity;

    @NotNull(message = "L'ID de l'entrepôt est obligatoire")
    private Integer warehouseId;
}