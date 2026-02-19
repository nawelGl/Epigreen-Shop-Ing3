package ms_product.domain.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "product_stock")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Stock {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_stock")
    private Integer id;

    @NotNull(message = "Le produit est obligatoire")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "id_catalog_product", nullable = false)
    private Product product;

    @NotBlank(message = "La taille est obligatoire")
    @Size(max = 10)
    @Column(name = "size_label", nullable = false, length = 10)
    private String sizeLabel;

    @NotNull
    @Min(value = 0, message = "La quantité ne peut pas être négative")
    @Column(name = "quantity_available", nullable = false)
    @Builder.Default
    private Integer quantity = 0;
}