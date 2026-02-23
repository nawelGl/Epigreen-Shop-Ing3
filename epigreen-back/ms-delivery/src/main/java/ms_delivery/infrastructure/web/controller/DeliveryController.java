package ms_delivery.infrastructure.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ms_delivery.application.dto.DeliveryCheckoutDTO;
import ms_delivery.application.dto.DeliveryResponseDTO;
import ms_delivery.application.mapper.DeliveryMapper;
import ms_delivery.application.service.DeliveryService;
import ms_delivery.domain.entity.Delivery;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/delivery")
public class DeliveryController {

    @Autowired
    private DeliveryService deliveryService;

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
}