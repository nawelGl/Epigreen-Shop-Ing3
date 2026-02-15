package ms_membership.application.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ms_membership.application.dto.CustomerRequestDTO;
import ms_membership.application.dto.CustomerResponseDTO;
import ms_membership.application.mapper.CustomerMapper;
import ms_membership.domain.entity.Customer;
import ms_membership.domain.repository.CustomerRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CustomerService {

    private final CustomerRepository repository;
    private final CustomerMapper mapper;

    /**
     * Crée un nouveau client.
     */
    @Transactional
    public CustomerResponseDTO createCustomer(CustomerRequestDTO request) {
        // 1. Vérifier si l'email existe déjà
        if (repository.existsByEmail(request.getEmail())) {
            throw new IllegalArgumentException("Cet email est déjà utilisé : " + request.getEmail());
        }

        // 2. Convertir DTO -> Entity
        Customer customer = mapper.toEntity(request);

        // TODO: Ajouter le hachage du mot de passe ici plus tard
        // Pour l'instant, on stocke en clair (MVP)
        customer.setPasswordHash(request.getPassword());

        // 3. Sauvegarder
        Customer savedCustomer = repository.save(customer);

        // 4. Retourner la réponse
        return mapper.toDto(savedCustomer);
    }

    /**
     * Récupère un client par son ID.
     */
    @Transactional(readOnly = true)
    public CustomerResponseDTO getCustomerById(Long id) {
        return repository.findById(id)
                .map(mapper::toDto)
                .orElseThrow(() -> new EntityNotFoundException("Client introuvable avec l'ID : " + id));
    }

    /**
     * Récupère les clients avec pagination.
     * 
     * @param pageable contient le numéro de page et la taille (ex: page=0, size=10)
     */
    @Transactional(readOnly = true)
    public Page<CustomerResponseDTO> getAllCustomers(Pageable pageable) {
        return repository.findAll(pageable)
                .map(mapper::toDto);
    }

    /**
     * Met à jour un client existant.
     */
    @Transactional
    public CustomerResponseDTO updateCustomer(Long id, CustomerRequestDTO request) {
        // 1. Récupérer le client existant
        Customer existingCustomer = repository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Client introuvable avec l'ID : " + id));

        // 2. Mettre à jour les champs (via le mapper)
        mapper.updateEntityFromDto(request, existingCustomer);

        // Note: On ne touche pas au mot de passe ici pour simplifier.
        // Idéalement, il faudrait une méthode dédiée changePassword().

        // 3. Sauvegarder (le save est implicite avec @Transactional mais on le met pour
        // être explicite)
        Customer updatedCustomer = repository.save(existingCustomer);

        return mapper.toDto(updatedCustomer);
    }

    /**
     * Supprime un client.
     */
    @Transactional
    public void deleteCustomer(Long id) {
        if (!repository.existsById(id)) {
            throw new EntityNotFoundException("Client introuvable avec l'ID : " + id);
        }
        repository.deleteById(id);
    }
}