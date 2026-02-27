package ms_product.application.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductResponseDTO {
    private Integer id;
    private String reference;
    private String name;
    private String brand;
    private String color;
    private String season;
    private String sizes;
    private String genderSegment;
    private String mainCategory;
    private String subCategory;
    private String articleType;
    private Integer scoreEc;
    private String scoreLabel;
    private Double price;
}