package ms_product.domain.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.*;

@Entity
@Table(name = "ref_product_catalog")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_catalog_product")
    private Integer id;

    @NotBlank(message = "La référence est obligatoire")
    @Size(max = 50)
    @Column(name = "reference", nullable = false)
    private String reference;

    @Size(max = 255)
    @Column(name = "name")
    private String name;

    @Size(max = 100)
    @Column(name = "brand")
    private String brand;

    @Size(max = 50)
    @Column(name = "color")
    private String color;

    @Size(max = 50)
    @Column(name = "season")
    private String season;

    @Column(name = "sizes")
    private String sizes;

    @Size(max = 50)
    @Column(name = "gender_segment")
    private String genderSegment;

    @Size(max = 50)
    @Column(name = "main_category")
    private String mainCategory;

    @Size(max = 50)
    @Column(name = "sub_category")
    private String subCategory;

    @Size(max = 50)
    @Column(name = "article_type")
    private String articleType;

    @Column(name = "score_ec")
    @Builder.Default
    private Integer scoreEc = 0;

    @Column(name = "price")
    private Double price;
}