package ms_delivery.infrastructure.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ms_delivery.application.dto.DeliveryCheckoutDTO;
import ms_delivery.application.dto.DeliveryCreateDTO;
import ms_delivery.application.dto.DeliveryResponseDTO;
import ms_delivery.application.mapper.DeliveryMapper;
import ms_delivery.application.service.DeliveryService;
import ms_delivery.application.service.geoapify.GeoapifyService;
import ms_delivery.domain.entity.Delivery;
import ms_delivery.domain.entity.DeliveryStatus;

import java.util.List;
import java.util.stream.Collectors;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/api/delivery")
public class DeliveryController {

    @Autowired
    private DeliveryService deliveryService;
    @Autowired
    private GeoapifyService geoapifyService;

    @Autowired
    private DeliveryMapper deliveryMapper;

    @GetMapping("/{id}")
    public ResponseEntity<DeliveryResponseDTO> getDeliveryById(@PathVariable Long id) {
        Delivery delivery = deliveryService.findById(id);
        if (delivery == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(deliveryMapper.toResponseDTO(delivery), HttpStatus.OK);
    }

    @GetMapping("/all")
    public ResponseEntity<List<DeliveryResponseDTO>> getAllDeliveries() {
        List<Delivery> deliveries = deliveryService.findAll();
        List<DeliveryResponseDTO> dtos = deliveries.stream()
                .map(deliveryMapper::toResponseDTO)
                .collect(Collectors.toList());

        return new ResponseEntity<>(dtos, HttpStatus.OK);
    }

    @PostMapping("/checkout")
    public ResponseEntity<DeliveryResponseDTO> finalizeDelivery(@RequestBody DeliveryCheckoutDTO checkoutDTO) {
        // On passe juste l'ID et la méthode au service
        Delivery deliveryInfo = new Delivery();
        deliveryInfo.setId(checkoutDTO.getDeliveryId());
        deliveryInfo.setDeliveryMethod(checkoutDTO.getDeliveryMethod());

        Delivery updated = deliveryService.finalizeDeliverySetup(deliveryInfo);

        if (updated == null) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(deliveryMapper.toResponseDTO(updated), HttpStatus.OK);
    }

    // IoT : Met à jour la position et renvoie le DTO propre
    @PutMapping("/{id}/location")
    public ResponseEntity<DeliveryResponseDTO> updateIoTLocation(
            @PathVariable Long id,
            @RequestParam Double lat,
            @RequestParam Double lon) {

        Delivery updated = deliveryService.updateCurrentLocation(id, lat, lon);

        if (updated == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(deliveryMapper.toResponseDTO(updated), HttpStatus.OK);
    }

    // Route pour créer la livraison initiale
    @PostMapping("/create")
    public ResponseEntity<DeliveryResponseDTO> createDelivery(@RequestBody DeliveryCreateDTO createDTO) {
        Delivery created = deliveryService.createDelivery(createDTO);
        return new ResponseEntity<>(deliveryMapper.toResponseDTO(created), HttpStatus.CREATED);
    }

    @GetMapping("/autocomplete")
    public ResponseEntity<List<String>> getAutocomplete(
            @RequestParam(required = false, defaultValue = "") String address) {

        if (address.trim().length() < 3) {
            return new ResponseEntity<>(List.of(), HttpStatus.OK);
        }

        List<String> suggestions = geoapifyService.autocompleteAddress(address);
        return new ResponseEntity<>(suggestions, HttpStatus.OK);
    }

    @PatchMapping("/{id}/status")
    public ResponseEntity<DeliveryResponseDTO> forceStatus(
            @PathVariable Long id, 
            @RequestParam DeliveryStatus status) {
        
        Delivery updated = deliveryService.updateStatus(id, status);
        
        if (updated == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(deliveryMapper.toResponseDTO(updated), HttpStatus.OK);
    }

    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<DeliveryResponseDTO>> getDeliveriesByCustomer(@PathVariable Long customerId) {
        List<Delivery> deliveries = deliveryService.findByCustomerId(customerId);
        List<DeliveryResponseDTO> dtos = deliveries.stream()
                .map(deliveryMapper::toResponseDTO)
                .collect(Collectors.toList());
        return new ResponseEntity<>(dtos, HttpStatus.OK);
    }

    @PostMapping("/update-location")
    public ResponseEntity<Void> updateLocation(@RequestBody String payload) {
        try {
            deliveryService.updateCurrentLocation(payload);
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}