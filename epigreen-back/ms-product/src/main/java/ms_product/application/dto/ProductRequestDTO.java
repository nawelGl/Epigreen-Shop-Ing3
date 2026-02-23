package ms_product.application.dto;

import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductRequestDTO {

    @NotBlank(message = "La référence est obligatoire")
    @Size(max = 50)
    private String reference;

    @Size(max = 255)
    private String name;

    @Size(max = 100)
    private String brand;

    @Size(max = 50)
    private String color;

    @Size(max = 50)
    private String season;

    @Size(max = 100)
    private String sizes;

    @Size(max = 50)
    private String genderSegment;

    @Size(max = 50)
    private String mainCategory;

    @Size(max = 50)
    private String subCategory;

    @Size(max = 50)
    private String articleType;

    private Integer scoreEc;

    @NotNull(message = "Le prix est obligatoire")
    @DecimalMin(value = "0.0", inclusive = false, message = "Le prix doit être positif")
    private Double price;
}