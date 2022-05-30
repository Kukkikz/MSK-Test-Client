import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsumerAvro {
    public static void main(String[] args) {
        System.out.println("ConsumerAvro");
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
        String grp_id = "kiktest1";

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaSaslProp.getProperty("brokers"));
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grp_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // Confluent Schema Registry for Java
        properties.put("basic.auth.credentials.source", "USER_INFO");
        properties.put("schema.registry.basic.auth.user.info", kafkaSaslProp.getProperty("basic.auth.user.info"));
        properties.put("schema.registry.url", kafkaSaslProp.getProperty("schema.registry.url"));

        /* Sadly, we need to do some string parsing to deal with 'poison pill' records (i.e. any message that cannot be
        de-serialized by KafkaAvroDeserializer, most likely because they weren't produced using Schema Registry) so we
        need to set up some regex things
         */
        final Pattern offsetPattern = Pattern.compile("\\w*offset*\\w[ ]\\d+");
        final Pattern partitionPattern = Pattern.compile("\\w*" + topic + "*\\w[-]\\d+");

        final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("Connecting to Kafka");


        while (true) {
            try {
                final ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (final ConsumerRecord<String, GenericRecord> record : records) {
                    try {
                        final String key = record.key();
                        final GenericRecord value = record.value();
                        System.out.println("Received message: key = " + key + ", value = " + value);
                    } catch (ClassCastException e) {
                        System.out.println("Record is not the specified type ... skipping");
                    }

                }
            } catch (final SerializationException e) {
                String text = e.getMessage();
                // Parse the error message to get the partition number and offset, in order to `seek` past the poison pill.
                Matcher mPart = partitionPattern.matcher(text);
                Matcher mOff = offsetPattern.matcher(text);

                mPart.find();
                Integer partition = Integer.parseInt(mPart.group().replace(topic + "-", ""));
                mOff.find();
                Long offset = Long.parseLong(mOff.group().replace("offset ", ""));
                System.out.println(String.format(
                        "'Poison pill' found at partition {0}, offset {1} .. skipping", partition, offset));
                consumer.seek(new TopicPartition(topic, partition), offset + 1);
                // Continue on
            }
        }
    }
}
