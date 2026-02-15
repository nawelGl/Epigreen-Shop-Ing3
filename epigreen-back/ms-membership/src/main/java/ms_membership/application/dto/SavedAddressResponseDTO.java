package ms_membership.application.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO pour la réponse d'une adresse sauvegardée.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SavedAddressResponseDTO {

    private Long id;
    private String street;
    private String city;
    private String zipCode;
    private String country;
    private Double gpsLat;
    private Double gpsLong;
}