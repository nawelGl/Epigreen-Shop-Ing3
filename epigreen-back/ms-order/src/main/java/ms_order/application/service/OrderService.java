package ms_order.application.service;

import lombok.RequiredArgsConstructor;
import ms_order.application.dto.*;
import ms_order.application.mapper.OrderMapper;
import ms_order.domain.entity.*;
import ms_order.domain.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import jakarta.persistence.EntityNotFoundException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final RestTemplate restTemplate = new RestTemplate();
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    @Value("${cart.service.url}")
    private String cartServiceUrl;

    @Transactional
    public OrderResponseDTO createOrder(OrderRequestDTO request) {
        // Appel HTTP  vers ms-cart
        String url = cartServiceUrl + "/api/cart/" + request.getCustomerId() + "/items";
        CartItemDTO[] itemsArray = restTemplate.getForObject(url, CartItemDTO[].class);

        if (itemsArray == null || itemsArray.length == 0) {
            throw new IllegalArgumentException("Le panier est vide.");
        }
        List<CartItemDTO> cartItems = Arrays.asList(itemsArray);

        Order order = orderMapper.toEntity(request);
        order.setStatus("PENDING"); // statut pending lors de creation
        BigDecimal totalPrice = BigDecimal.ZERO;

        for (CartItemDTO cartItem : cartItems) {
            OrderItem orderItem = new OrderItem();
            orderItem.setOrder(order);
            orderItem.setProductId(cartItem.getProductId());
            orderItem.setSizeLabel(cartItem.getSizeLabel() != null ? cartItem.getSizeLabel() : "STANDARD"); // STANDARD si vide
            orderItem.setQuantity(cartItem.getQuantity());
            orderItem.setUnitPrice(cartItem.getPrice());
            orderItem.setWarehouseId(1); 

            order.getItems().add(orderItem);
            
            BigDecimal lineTotal = cartItem.getPrice().multiply(new BigDecimal(cartItem.getQuantity()));
            totalPrice = totalPrice.add(lineTotal);
        }

        order.setTotalPrice(totalPrice);
        Order savedOrder = orderRepository.save(order);
        log.info("Commande créée pour un montant total de " + order.getTotalPrice() + "€.");
        try {
            String deleteUrl = cartServiceUrl + "/api/cart/" + request.getCustomerId();
            
            // la suppression
            restTemplate.delete(deleteUrl); 
            log.info("Panier vidé avec succès pour l'ID: " + request.getCustomerId());
            
        } catch (Exception e) {
            System.err.println("Erreur lors du vidage du panier: " + e.getMessage());
        }

        return orderMapper.toDto(savedOrder);
    }

    @Transactional(readOnly = true)
    public OrderResponseDTO getOrderById(Integer id) {
        return orderRepository.findById(id)
                .map(orderMapper::toDto)
                .orElseThrow(() -> new EntityNotFoundException("Commande introuvable avec l'ID : " + id));
    }
}