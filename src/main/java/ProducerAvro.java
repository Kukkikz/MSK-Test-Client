import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerAvro {
    public static void main(String[] args) {
        System.out.println("ProducerAvro");
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
        String topic = "test-avro";
        String schemaName = "test-avro";
        String registryName = "kiktest-registry";

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaSaslProp.getProperty("brokers"));
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
        properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, schemaName); // If not passed, uses transport name (topic name in case of Kafka, or stream name in case of Kinesis Data Streams)
        properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, registryName); // If not passed, uses "default-registry"

        Schema schema_user = null;
        try {
            schema_user = new org.apache.avro.Schema.Parser().parse(new File("src/main/resources/user.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenericRecord user = new GenericData.Record(schema_user);
        user.put("name", "Bob");
        user.put("age", 45);

        List<GenericRecord> users = new ArrayList<>();
        users.add(user);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties)) {
            for (int i = 0; i < users.size(); i++) {
                GenericRecord r = users.get(i);

                final ProducerRecord<String, GenericRecord> record;
                record = new ProducerRecord<String, GenericRecord>(topic, r.get("name").toString(), r);

                producer.send(record);
                System.out.println("Sent message " + (i + 1));
                Thread.sleep(1000L);
            }
            producer.flush();
            System.out.println("Successfully produced messages to a topic called " + topic);

        } catch (final InterruptedException | SerializationException e) {
            e.printStackTrace();
        }
    }
}
