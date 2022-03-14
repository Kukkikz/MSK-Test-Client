import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        System.out.println("Run Producer");
        Properties kafkaSaslProp = new Properties();
        String fileName = "src/main/resources/kafka.config";
        try (FileInputStream fis = new FileInputStream(fileName)) {
            kafkaSaslProp.load(fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        String username = kafkaSaslProp.getProperty("saslUsername");
        String password = kafkaSaslProp.getProperty("saslPassword");
        String topic = "MSKTutorialTopic";
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaSaslProp.getProperty("brokers"));
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> first_producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record=new ProducerRecord<String, String>(topic, "Hi Kafka 2");
        first_producer.send(record);
        first_producer.flush();
        first_producer.close();
    }


}
