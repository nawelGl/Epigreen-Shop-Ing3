package ms_order.application.mapper;

import ms_order.application.dto.OrderRequestDTO;
import ms_order.application.dto.OrderResponseDTO;
import ms_order.domain.entity.Order;
import org.springframework.stereotype.Component;

@Component
public class OrderMapper {

    public Order toEntity(OrderRequestDTO dto) {
        if (dto == null) return null;
        Order order = new Order();
        order.setCustomerId(dto.getCustomerId());
        order.setShippingStreet(dto.getShippingStreet());
        order.setShippingCity(dto.getShippingCity());
        order.setShippingZipCode(dto.getShippingZipCode());
        order.setShippingCountry(dto.getShippingCountry());
        return order;
    }

    public OrderResponseDTO toDto(Order order) {
        if (order == null) return null;
        OrderResponseDTO dto = new OrderResponseDTO();
        dto.setId(order.getId());
        dto.setCustomerId(order.getCustomerId());
        dto.setStatus(order.getStatus());
        dto.setTotalPrice(order.getTotalPrice());
        dto.setShippingStreet(order.getShippingStreet());
        dto.setShippingCity(order.getShippingCity());
        dto.setShippingZipCode(order.getShippingZipCode());
        dto.setShippingCountry(order.getShippingCountry());
        dto.setCreatedAt(order.getCreatedAt());
        return dto;
    }
    public void updateEntityFromDto(OrderRequestDTO dto, Order order) {
        if (dto == null) return;
        order.setShippingStreet(dto.getShippingStreet());
        order.setShippingCity(dto.getShippingCity());
        order.setShippingZipCode(dto.getShippingZipCode());
        if (dto.getShippingCountry() != null) {
            order.setShippingCountry(dto.getShippingCountry());
        }
    }
}