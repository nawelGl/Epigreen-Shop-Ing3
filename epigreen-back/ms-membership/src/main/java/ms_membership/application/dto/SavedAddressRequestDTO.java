package ms_membership.application.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO pour la création/modification d'une adresse.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SavedAddressRequestDTO {

    @NotBlank(message = "La rue ne peut pas être vide")
    private String street;

    @NotBlank(message = "La ville ne peut pas être vide")
    private String city;

    @NotBlank(message = "Le code postal ne peut pas être vide")
    private String zipCode;

    @NotBlank(message = "Le pays ne peut pas être vide")
    private String country;

    private Double gpsLat;
    private Double gpsLong;
}