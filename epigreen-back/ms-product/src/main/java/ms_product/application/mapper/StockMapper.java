package ms_product.application.mapper;

import org.springframework.stereotype.Component;
import ms_product.application.dto.StockRequestDTO;
import ms_product.application.dto.StockResponseDTO;
import ms_product.domain.entity.Product;
import ms_product.domain.entity.Stock;
import ms_product.domain.entity.Warehouse;

@Component
public class StockMapper {

    /**
     * Convertit un DTO en entité Stock.
     * Le produit doit être récupéré par le Service et passé en paramètre.
     */
    public Stock toEntity(StockRequestDTO dto, Product product, Warehouse warehouse) {
        if (dto == null)
            return null;

        return Stock.builder()
                .product(product)
                .sizeLabel(dto.getSizeLabel())
                .quantity(dto.getQuantity() != null ? dto.getQuantity() : 0)
                .warehouse(warehouse)
                .build();
    }

    /**
     * Convertit l'entité Stock en DTO pour la réponse API.
     */
    public StockResponseDTO toDto(Stock stock) {
        if (stock == null)
            return null;

        return StockResponseDTO.builder()
                .id(stock.getId())
                // On extrait juste l'ID du produit pour le DTO
                .productId(stock.getProduct() != null ? stock.getProduct().getId() : null)
                .sizeLabel(stock.getSizeLabel())
                .quantity(stock.getQuantity())
                .warehouseId(stock.getWarehouse() != null ? stock.getWarehouse().getId() : null)
                .build();
    }

    /**
     * Met à jour uniquement les informations modifiables du stock.
     */
    public void updateEntityFromDto(StockRequestDTO dto, Stock stock) {
        if (dto == null || stock == null)
            return;

        stock.setSizeLabel(dto.getSizeLabel());
        if (dto.getQuantity() != null) {
            stock.setQuantity(dto.getQuantity());
        }
    }
}