package ms_membership.application.mapper;


import lombok.RequiredArgsConstructor;
import ms_membership.application.dto.CustomerRequestDTO;
import ms_membership.application.dto.CustomerResponseDTO;
import ms_membership.domain.entity.Customer;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Mapper pour convertir entre Customer et ses DTOs.
 * Best practices :
 * - Injection du mapper enfant (AddressMapper)
 * - Gestion des collections nulles
 * - Le mot de passe n'est PAS mappé ici (responsabilité du Service)
 */
@Component
@RequiredArgsConstructor
public class CustomerMapper {

    private final SavedAddressMapper addressMapper;

    /**
     * Convertit un CustomerRequestDTO en entité Customer.
     * Note: Le passwordHash doit être défini par le Service via un PasswordEncoder.
     */
    public Customer toEntity(CustomerRequestDTO dto) {
        return Customer.builder()
                .firstName(dto.getFirstName())
                .lastName(dto.getLastName())
                .email(dto.getEmail())
                .birthDate(dto.getBirthDate())
                .gender(dto.getGender())
                .savedAddresses(Collections.emptyList())
                .build();
    }

    /**
     * Convertit une entité Customer en CustomerResponseDTO
     */
    public CustomerResponseDTO toDto(Customer customer) {
        if (customer == null) {
            return null;
        }

        return CustomerResponseDTO.builder()
                .id(customer.getId())
                .firstName(customer.getFirstName())
                .lastName(customer.getLastName())
                .email(customer.getEmail())
                .birthDate(customer.getBirthDate())
                .gender(customer.getGender())
                .createdAt(customer.getCreatedAt())
                .savedAddresses(
                        customer.getSavedAddresses() != null ? customer.getSavedAddresses().stream()
                                .map(addressMapper::toDto)
                                .collect(Collectors.toList())
                                : Collections.emptyList())
                .build();
    }

    /**
     * Met à jour une entité Customer existante avec les données du DTO.
     * Note: On ne met pas à jour l'email (unique) ni le mot de passe ici.
     */
    public void updateEntityFromDto(CustomerRequestDTO dto, Customer customer) {
        customer.setFirstName(dto.getFirstName());
        customer.setLastName(dto.getLastName());
        customer.setBirthDate(dto.getBirthDate());
        customer.setGender(dto.getGender());
    }
}