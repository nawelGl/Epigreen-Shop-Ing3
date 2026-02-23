package ms_product.domain.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import ms_product.domain.entity.Product;

import java.util.Optional;

import java.util.List;
@Repository
public interface ProductRepository extends JpaRepository<Product, Integer> {

    Optional<Product> findByReference(String reference);

    Page<Product> findByMainCategory(String mainCategory, Pageable pageable);

    boolean existsByReference(String reference);


    // Jointure pour récupérer les produits recommendés
    @Query(value = "SELECT p.* FROM ref_product_catalog p " +
                   "INNER JOIN product_recommendation pr ON p.id_catalog_product = pr.id_product_ref " +
                   "WHERE pr.id_customer_ref = :userId " +
                   "ORDER BY pr.affinity_score DESC LIMIT 4", 
           nativeQuery = true)
    List<Product> findRecommendationsForUser(@Param("userId") Long userId);
}