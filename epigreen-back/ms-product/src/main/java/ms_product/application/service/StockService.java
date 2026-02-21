package ms_product.application.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import ms_product.application.dto.StockRequestDTO;
import ms_product.application.dto.StockResponseDTO;
import ms_product.application.mapper.StockMapper;
import ms_product.domain.entity.Product;
import ms_product.domain.entity.Stock;
import ms_product.domain.repository.ProductRepository;
import ms_product.domain.repository.StockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class StockService {

    private final StockRepository stockRepository;
    private final ProductRepository productRepository;
    private final StockMapper stockMapper;

    /**
     * Définit ou met à jour le stock pour un produit et une taille donnés.
     */
    @Transactional
    public StockResponseDTO setStock(StockRequestDTO request) {
        // 1. Vérifier que le produit existe bien
        Product product = productRepository.findById(request.getProductId())
                .orElseThrow(
                        () -> new EntityNotFoundException("Produit introuvable avec l'ID : " + request.getProductId()));

        // 2. Chercher si on a déjà une ligne de stock pour ce produit + cette taille
        Optional<Stock> existingStockOpt = stockRepository.findByProductIdAndSizeLabel(
                request.getProductId(), request.getSizeLabel());

        Stock stockToSave;

        if (existingStockOpt.isPresent()) {
            // Mise à jour de la ligne existante
            stockToSave = existingStockOpt.get();
            stockToSave.setQuantity(request.getQuantity());
        } else {
            // Création d'une nouvelle ligne
            stockToSave = stockMapper.toEntity(request, product);
        }

        Stock savedStock = stockRepository.save(stockToSave);
        return stockMapper.toDto(savedStock);
    }

    /**
     * Récupère tout le stock disponible pour un produit donné (toutes les tailles).
     */
    @Transactional(readOnly = true)
    public List<StockResponseDTO> getStockByProductId(Integer productId) {
        // On vérifie d'abord que le produit existe
        if (!productRepository.existsById(productId)) {
            throw new EntityNotFoundException("Produit introuvable avec l'ID : " + productId);
        }

        // Requête optimisée grâce à @EntityGraph
        return stockRepository.findByProductId(productId).stream()
                .map(stockMapper::toDto)
                .collect(Collectors.toList());
    }
}