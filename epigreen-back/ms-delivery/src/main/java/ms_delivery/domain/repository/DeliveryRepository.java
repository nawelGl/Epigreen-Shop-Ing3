package ms_delivery.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ms_delivery.domain.entity.Delivery;
import java.util.Optional;

@Repository
public interface DeliveryRepository extends JpaRepository<Delivery, Long> {
    Optional<Delivery> findByOrderId(Long orderId);

    Optional<Delivery> findByTrackingNumber(String trackingNumber);
}