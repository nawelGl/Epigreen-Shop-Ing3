package ms_membership.domain.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Entité SavedAddress représentant le carnet d'adresses d'un client.
 * Mappe la table "saved_addresses".
 */
@Entity
@Table(name = "saved_addresses")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SavedAddress {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Clé étrangère vers la table customers
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "customer_id", nullable = false)
    private Customer customer;

    @Column(name = "street", columnDefinition = "TEXT")
    private String street;

    @Column(name = "city", columnDefinition = "TEXT")
    private String city;

    @Column(name = "zip_code", columnDefinition = "TEXT")
    private String zipCode;

    @Column(name = "country", columnDefinition = "TEXT")
    private String country;

    @Column(name = "gps_lat")
    private Double gpsLat;

    @Column(name = "gps_long")
    private Double gpsLong;
}
