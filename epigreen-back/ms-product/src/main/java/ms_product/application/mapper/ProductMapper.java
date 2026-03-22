package ms_product.application.mapper;

import org.springframework.stereotype.Component;
import ms_product.application.dto.ProductRequestDTO;
import ms_product.application.dto.ProductResponseDTO;
import ms_product.domain.entity.Product;

@Component
public class ProductMapper {

    /**
     * Convertit un ProductRequestDTO en entité Product
     */
    public Product toEntity(ProductRequestDTO dto) {
        if (dto == null)
            return null;

        return Product.builder()
                .reference(dto.getReference())
                .name(dto.getName())
                .brand(dto.getBrand())
                .color(dto.getColor())
                .season(dto.getSeason())
                .sizes(dto.getSizes())
                .genderSegment(dto.getGenderSegment())
                .mainCategory(dto.getMainCategory())
                .subCategory(dto.getSubCategory())
                .articleType(dto.getArticleType())
                .scoreEc(dto.getScoreEc() != null ? dto.getScoreEc() : 0)
                .scoreLabel(dto.getScoreLabel())
                .price(dto.getPrice())
                .build();
    }

    /**
     * Convertit une entité Product en ProductResponseDTO
     */
    public ProductResponseDTO toDto(Product product) {
        if (product == null)
            return null;

        return ProductResponseDTO.builder()
                .id(product.getId())
                .reference(product.getReference())
                .name(product.getName())
                .brand(product.getBrand())
                .color(product.getColor())
                .season(product.getSeason())
                .sizes(product.getSizes())
                .genderSegment(product.getGenderSegment())
                .mainCategory(product.getMainCategory())
                .subCategory(product.getSubCategory())
                .articleType(product.getArticleType())
                .scoreEc(product.getScoreEc())
                .scoreLabel(product.getScoreLabel())
                .price(product.getPrice())
                .build();
    }

    /**
     * Met à jour une entité Product existante avec les données du DTO
     */
    public void updateEntityFromDto(ProductRequestDTO dto, Product product) {
        if (dto == null || product == null)
            return;

        product.setReference(dto.getReference());
        product.setName(dto.getName());
        product.setBrand(dto.getBrand());
        product.setColor(dto.getColor());
        product.setSeason(dto.getSeason());
        product.setSizes(dto.getSizes());
        product.setGenderSegment(dto.getGenderSegment());
        product.setMainCategory(dto.getMainCategory());
        product.setSubCategory(dto.getSubCategory());
        product.setArticleType(dto.getArticleType());
        if (dto.getScoreEc() != null) {
            product.setScoreEc(dto.getScoreEc());
        }
        product.setScoreLabel(dto.getScoreLabel());
        product.setPrice(dto.getPrice());
    }
}