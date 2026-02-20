package ms_product.application.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import ms_product.application.dto.ProductRequestDTO;
import ms_product.application.dto.ProductResponseDTO;
import ms_product.application.mapper.ProductMapper;
import ms_product.domain.entity.Product;
import ms_product.domain.repository.ProductRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    @Transactional
    public ProductResponseDTO createProduct(ProductRequestDTO request) {
        // Règle métier : On ne peut pas avoir deux produits avec la même référence
        if (productRepository.existsByReference(request.getReference())) {
            throw new IllegalArgumentException("La référence produit existe déjà : " + request.getReference());
        }

        Product product = productMapper.toEntity(request);
        Product savedProduct = productRepository.save(product);
        return productMapper.toDto(savedProduct);
    }

    @Transactional(readOnly = true)
    public ProductResponseDTO getProductById(Integer id) {
        return productRepository.findById(id)
                .map(productMapper::toDto)
                .orElseThrow(() -> new EntityNotFoundException("Produit introuvable avec l'ID : " + id));
    }

    @Transactional(readOnly = true)
    public Page<ProductResponseDTO> getAllProducts(Pageable pageable) {
        return productRepository.findAll(pageable)
                .map(productMapper::toDto);
    }

    @Transactional
    public ProductResponseDTO updateProduct(Integer id, ProductRequestDTO request) {
        Product existingProduct = productRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Produit introuvable avec l'ID : " + id));

        // Vérification anti-doublon si la référence change
        if (!existingProduct.getReference().equals(request.getReference()) &&
                productRepository.existsByReference(request.getReference())) {
            throw new IllegalArgumentException("La nouvelle référence est déjà utilisée.");
        }

        productMapper.updateEntityFromDto(request, existingProduct);
        Product updatedProduct = productRepository.save(existingProduct);

        return productMapper.toDto(updatedProduct);
    }

    @Transactional
    public void deleteProduct(Integer id) {
        if (!productRepository.existsById(id)) {
            throw new EntityNotFoundException("Produit introuvable avec l'ID : " + id);
        }
        productRepository.deleteById(id);
    }
}