package ms_telemetry.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic deliveryLocationTopic() {
        return TopicBuilder.name("gps-data-raw")
                .partitions(1)
                .replicas(1)
                .build();
    }
}