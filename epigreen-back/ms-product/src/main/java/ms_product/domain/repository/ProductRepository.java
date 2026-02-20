package ms_product.domain.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import ms_product.domain.entity.Product;

import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, Integer> {

    Optional<Product> findByReference(String reference);

    Page<Product> findByMainCategory(String mainCategory, Pageable pageable);

    boolean existsByReference(String reference);
}