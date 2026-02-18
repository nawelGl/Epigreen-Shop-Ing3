package ms_cart.application.service;

import ms_cart.application.dto.CartItemRequestDTO;
import ms_cart.domain.entity.Cart;
import ms_cart.domain.entity.CartItem;
import ms_cart.domain.repository.CartRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CartService {
    private final CartRepository cartRepository;

    public Cart addToCart(String userId, CartItemRequestDTO request) {
        // verifier si on a deja un panier pour un user
        Cart cart = cartRepository.findById(userId)
                .orElse(Cart.builder().userId(userId).build());

        Optional<CartItem> existingItem = cart.getItems().stream()
                .filter(item -> item.getProductId().equals(request.getProductId()))
                .findFirst();
        // verifer si on a deja un item si oui augmenter la quantite
        if (existingItem.isPresent()) {
            CartItem item = existingItem.get();
            item.setQuantity(item.getQuantity() + request.getQuantity());
        } else { // ou ajouter un item
            CartItem newItem = CartItem.builder()
                    .productId(request.getProductId())
                    .productName(request.getProductName())
                    .price(request.getPrice())
                    .quantity(request.getQuantity())
                    .build();
            cart.getItems().add(newItem);
        }
        cart.calculateTotalPrice();
        return cartRepository.save(cart);
    }

    public Cart getCart(String userId) {
        return cartRepository.findById(userId)
                .orElse(Cart.builder().userId(userId).build());
    }

    public void clearCart(String userId) {
        cartRepository.deleteById(userId);
    }
}