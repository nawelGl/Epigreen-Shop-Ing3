package ms_cart.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import java.util.ArrayList;
import java.util.List;

// on utilise un prefix "cart", timeToLive= 7j en sec
@RedisHash(value = "cart", timeToLive = 604800)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Cart {
    @Id
    private String userId;

    @Builder.Default
    private List<CartItem> items = new ArrayList<>();

    private Double totalPrice;


    // ToDo: a changer apres le dev de product
    public void calculateTotalPrice() {
        this.totalPrice = this.items.stream()
                .mapToDouble(item -> item.getPrice() * item.getQuantity())
                .sum();
    }
}