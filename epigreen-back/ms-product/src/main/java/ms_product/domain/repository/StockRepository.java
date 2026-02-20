package ms_product.domain.repository;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import ms_product.domain.entity.Stock;

import java.util.List;
import java.util.Optional;

@Repository
public interface StockRepository extends JpaRepository<Stock, Integer> {

    @EntityGraph(attributePaths = { "product" })
    List<Stock> findByProductId(Integer productId);

    @EntityGraph(attributePaths = { "product" })
    Optional<Stock> findByProductIdAndSizeLabel(Integer productId, String sizeLabel);

    boolean existsByProductIdAndSizeLabel(Integer productId, String sizeLabel);
}