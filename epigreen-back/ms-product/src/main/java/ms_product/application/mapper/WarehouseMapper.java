package ms_product.application.mapper;

import org.springframework.stereotype.Component;
import ms_product.application.dto.WarehouseRequestDTO;
import ms_product.application.dto.WarehouseResponseDTO;
import ms_product.domain.entity.Warehouse;

@Component
public class WarehouseMapper {

    public Warehouse toEntity(WarehouseRequestDTO dto) {
        if (dto == null)
            return null;

        return Warehouse.builder()
                .name(dto.getName())
                .city(dto.getCity())
                .street(dto.getStreet())
                .zipCode(dto.getZipCode())
                .country(dto.getCountry() != null ? dto.getCountry() : "France")
                .gpsLat(dto.getGpsLat())
                .gpsLong(dto.getGpsLong())
                .build();
    }

    public WarehouseResponseDTO toDto(Warehouse warehouse) {
        if (warehouse == null)
            return null;

        return WarehouseResponseDTO.builder()
                .id(warehouse.getId())
                .name(warehouse.getName())
                .city(warehouse.getCity())
                .street(warehouse.getStreet())
                .zipCode(warehouse.getZipCode())
                .country(warehouse.getCountry())
                .gpsLat(warehouse.getGpsLat())
                .gpsLong(warehouse.getGpsLong())
                .build();
    }

    public void updateEntityFromDto(WarehouseRequestDTO dto, Warehouse warehouse) {
        if (dto == null || warehouse == null)
            return;

        warehouse.setName(dto.getName());
        warehouse.setCity(dto.getCity());
        warehouse.setStreet(dto.getStreet());
        warehouse.setZipCode(dto.getZipCode());
        if (dto.getCountry() != null) {
            warehouse.setCountry(dto.getCountry());
        }
        warehouse.setGpsLat(dto.getGpsLat());
        warehouse.setGpsLong(dto.getGpsLong());
    }
}