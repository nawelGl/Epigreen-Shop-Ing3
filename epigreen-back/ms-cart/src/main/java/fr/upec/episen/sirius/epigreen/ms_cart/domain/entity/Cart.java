package main.java.fr.upec.episen.sirius.epigreen.ms_cart.domain.entity;

import java.lang.annotation.Inherited;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;



// on utilise un prefix "cart", timeToLive= 7j en sec
@RedisHash(value = "cart", timeToLive=604800)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Cart {

    @Id
    private String userId; // cle de redis

    @Builder.Defalut 
    private List<CartItem> items = new ArrayList<>();

    private Double totalPrices; // on calculera dans la service

    public void calculateTotalPrice(){
        this.totalPrice =this.items.stream()
                            .mapToDouble(item -> item.getPrice() * item.getQuantity())
                            .sum();

    }
    
}
