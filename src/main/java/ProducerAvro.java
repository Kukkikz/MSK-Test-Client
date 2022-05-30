
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerAvro {
    public static void main(String[] args) throws RestClientException, IOException {
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
        String topic = "kik-return-test";
        String schemaName = "kik-return-test-value";
        String registryName = "kiktest-registry";

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaSaslProp.getProperty("brokers"));
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());

        // Confluent Schema Registry for Java
        properties.put("basic.auth.credentials.source", "USER_INFO");
        properties.put("schema.registry.basic.auth.user.info", kafkaSaslProp.getProperty("basic.auth.user.info"));
        properties.put("schema.registry.url", kafkaSaslProp.getProperty("schema.registry.url"));

        // Optimize Performance for Confluent Cloud
//        properties.put(ProducerConfig.RETRIES_CONFIG, 2147483647);
//        properties.put("producer.confluent.batch.expiry.ms", 9223372036854775807);
//        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 300000);
//        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 9223372036854775807);

//        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(kafkaSaslProp.getProperty("schema.registry.url"), 20);
//        SchemaMetadata sm = client.getLatestSchemaMetadata(schemaName);
//        System.out.println(sm.getSchema());


        Schema schema_user = null;
        try {
            schema_user = new org.apache.avro.Schema.Parser().parse(new File("src/main/resources/user.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenericRecord user = new GenericData.Record(schema_user);
        user.put("Name", "Bob");
        user.put("Age", 28);



        List<GenericRecord> users = new ArrayList<>();
        users.add(user);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties)) {
            for (int i = 0; i < users.size(); i++) {
                GenericRecord r = users.get(i);
                final ProducerRecord<String, GenericRecord> record;
                record = new ProducerRecord<String, GenericRecord>(topic, r.get("Name").toString(), r);

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
