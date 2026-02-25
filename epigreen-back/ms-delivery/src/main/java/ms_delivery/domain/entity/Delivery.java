package ms_delivery.domain.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "deliveries")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, unique = true)
    private Long orderId;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Column(name = "origin_warehouse_id", nullable = false)
    private Long originWarehouseId;

    @Column(name = "tracking_number", nullable = false, unique = true)
    private String trackingNumber;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private DeliveryStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "delivery_method")
    private DeliveryMethod deliveryMethod;

    @Column(name = "dest_street", nullable = false)
    private String destStreet;

    @Column(name = "dest_city", nullable = false)
    private String destCity;

    @Column(name = "dest_zip_code", nullable = false)
    private String destZipCode;

    @Column(name = "dest_lat")
    private Double destLat;

    @Column(name = "dest_lon")
    private Double destLon;

    @Column(name = "origin_lat")
    private Double originLat;

    @Column(name = "origin_lon")
    private Double originLon;

    @Column(name = "current_lat")
    private Double currentLat;

    @Column(name = "current_lon")
    private Double currentLon;

    @Column(name = "calculated_distance")
    private Double calculatedDistance;

    @Column(name = "carbon_footprint")
    private Double carbonFootprint;

    @Enumerated(EnumType.STRING)
    @Column(name = "delivery_score")
    private DeliveryScore deliveryScore;

    @Column(name = "estimated_delivery_date")
    private LocalDateTime estimatedDeliveryDate;

    @Column(name = "actual_delivery_date")
    private LocalDateTime actualDeliveryDate;
}