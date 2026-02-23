package ms_product.infrastructure.web.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import ms_product.application.dto.StockRequestDTO;
import ms_product.application.dto.StockResponseDTO;
import ms_product.application.service.StockService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
public class StockController {

    private final StockService stockService;

    /**
     * Définit ou met à jour le stock pour un produit et une taille.
     */
    @PostMapping
    public ResponseEntity<StockResponseDTO> setStock(@Valid @RequestBody StockRequestDTO request) {
        // On utilise OK (200) car cette méthode peut faire un CREATE (201) ou un UPDATE
        // (200)
        return ResponseEntity.ok(stockService.setStock(request));
    }

    /**
     * Récupère toutes les tailles et quantités disponibles pour un produit.
     */
    @GetMapping("/product/{productId}")
    public ResponseEntity<List<StockResponseDTO>> getStockByProductId(@PathVariable Integer productId) {
        return ResponseEntity.ok(stockService.getStockByProductId(productId));
    }
}