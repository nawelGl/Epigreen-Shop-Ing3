package ms_membership.web.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import ms_membership.application.dto.CustomerRequestDTO;
import ms_membership.application.dto.CustomerResponseDTO;
import ms_membership.application.service.CustomerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/customers")
@RequiredArgsConstructor
public class CustomerController {

    private final CustomerService service;

    /**
     * POST /api/customers : Créer un nouveau client
     */
    @PostMapping
    public ResponseEntity<CustomerResponseDTO> createCustomer(@Valid @RequestBody CustomerRequestDTO request) {
        CustomerResponseDTO createdCustomer = service.createCustomer(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdCustomer);
    }

    /**
     * GET /api/customers/{id} : Récupérer un client par son ID
     */
    @GetMapping("/{id}")
    public ResponseEntity<CustomerResponseDTO> getCustomerById(@PathVariable Long id) {
        return ResponseEntity.ok(service.getCustomerById(id));
    }

    /**
     * GET /api/customers : Récupérer la liste de tous les clients
     */
    @GetMapping
    public ResponseEntity<List<CustomerResponseDTO>> getAllCustomers() {
        return ResponseEntity.ok(service.getAllCustomers());
    }

    /**
     * PUT /api/customers/{id} : Mettre à jour un client
     */
    @PutMapping("/{id}")
    public ResponseEntity<CustomerResponseDTO> updateCustomer(@PathVariable Long id,
            @Valid @RequestBody CustomerRequestDTO request) {
        return ResponseEntity.ok(service.updateCustomer(id, request));
    }

    /**
     * DELETE /api/customers/{id} : Supprimer un client
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteCustomer(@PathVariable Long id) {
        service.deleteCustomer(id);
        return ResponseEntity.noContent().build();
    }
}