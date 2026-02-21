package ms_cart.infrastructure.web.controller;

import ms_cart.application.dto.CartItemRequestDTO;
import ms_cart.application.service.CartService;
import ms_cart.domain.entity.Cart;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(origins = "*") // aceepter les navigateurs
@RequestMapping("/api/cart")
@RequiredArgsConstructor
public class CartController {
    private final CartService cartService;

    //ajtouter
    @PostMapping("/{userId}")
    public ResponseEntity<Cart> addItem(@PathVariable String userId, @RequestBody CartItemRequestDTO request) {
        return ResponseEntity.ok(cartService.addToCart(userId, request));
    }

    // Get 
    @GetMapping("/{userId}")
    public ResponseEntity<Cart> getCart(@PathVariable String userId) {
        return ResponseEntity.ok(cartService.getCart(userId));
    }
    //vider le panier
    @DeleteMapping("/{userId}")
    public ResponseEntity<Void> clearCart(@PathVariable String userId) {
        cartService.clearCart(userId);
        return ResponseEntity.noContent().build();
    }
}