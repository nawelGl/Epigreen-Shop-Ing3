package ms_telemetry;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import ms_telemetry.consumer.TelemetryConsumer;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class MqttToKafkaBridge {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
        private static final Logger log = LoggerFactory.getLogger(MqttToKafkaBridge.class);


    public MqttToKafkaBridge() throws MqttException {
        String publisherId = UUID.randomUUID().toString();
        IMqttClient client = new MqttClient("tcp://localhost:1883", publisherId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);

        log.info("Transmission des données de MQTT à Kafka (dans le topic gps-data-raw) ...");

        client.connect(options);

        // On écoute MQTT
        client.subscribe("delivery/location", (topic, msg) -> {
            String payload = new String(msg.getPayload());
            // On renvoie vers KAFKA
            kafkaTemplate.send("gps-data-raw", payload);
            //System.out.println("Bridge: MQTT -> Kafka : " + payload);
        });
    }
}