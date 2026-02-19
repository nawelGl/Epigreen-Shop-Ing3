package ms_membership.application.mapper;

import org.springframework.stereotype.Component;
import ms_membership.application.dto.SavedAddressRequestDTO;
import ms_membership.application.dto.SavedAddressResponseDTO;
import ms_membership.domain.entity.SavedAddress;

/**
 * Mapper pour convertir entre SavedAddress et ses DTOs.
 */
@Component
public class SavedAddressMapper {

    /**
     * Convertit un SavedAddressRequestDTO en entité SavedAddress
     */
    public SavedAddress toEntity(SavedAddressRequestDTO dto) {
        return SavedAddress.builder()
                .street(dto.getStreet())
                .city(dto.getCity())
                .zipCode(dto.getZipCode())
                .country(dto.getCountry())
                .gpsLat(dto.getGpsLat())
                .gpsLong(dto.getGpsLong())
                .build();
    }

    /**
     * Convertit une entité SavedAddress en SavedAddressResponseDTO
     */
    public SavedAddressResponseDTO toDto(SavedAddress address) {
        if (address == null) {
            return null;
        }
        return SavedAddressResponseDTO.builder()
                .id(address.getId())
                .street(address.getStreet())
                .city(address.getCity())
                .zipCode(address.getZipCode())
                .country(address.getCountry())
                .gpsLat(address.getGpsLat())
                .gpsLong(address.getGpsLong())
                .build();
    }

    /**
     * Met à jour une entité SavedAddress existante avec les données du DTO
     */
    public void updateEntityFromDto(SavedAddressRequestDTO dto, SavedAddress address) {
        address.setStreet(dto.getStreet());
        address.setCity(dto.getCity());
        address.setZipCode(dto.getZipCode());
        address.setCountry(dto.getCountry());
        address.setGpsLat(dto.getGpsLat());
        address.setGpsLong(dto.getGpsLong());
    }
}