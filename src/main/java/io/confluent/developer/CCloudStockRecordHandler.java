package io.confluent.developer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CCloudStockRecordHandler implements RequestHandler<Map<String, Object>, Void> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Object> configs = new HashMap<>();
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();

    public CCloudStockRecordHandler() {
        configs.putAll(getSecretsConfigs());
        configs.put("security.protocol", "SASL_SSL");
        configs.put("sasl.mechanism", "PLAIN");
        configs.put("basic.auth.credentials.source", "USER_INFO");
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "LambdaProducer");
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        stringDeserializer.configure(configs, false);
        kafkaAvroDeserializer.configure(configs, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void handleRequest(Map<String, Object> payload, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Configs are " + configs);
        Map<String, List<Map<String, String>>> records = (Map<String, List<Map<String, String>>>) payload.get("records");

        records.forEach((key, recordList) ->  {
                logger.log("Topic-Partition for this batch of records " + key +" number records in batch " + recordList.size());
            recordList.forEach(recordMap -> {
                byte[] keyBytes;
                String callSign = null;
                if (recordMap.containsKey("key")) {
                    keyBytes = decode(recordMap.get("key"));
                    callSign = stringDeserializer.deserialize("", keyBytes);
                }
                byte[] bytes = decode(recordMap.get("value"));
                GenericRecord flightDelay = (GenericRecord) kafkaAvroDeserializer.deserialize("", bytes);
                logger.log("Record key is " + callSign + " Record value is " + flightDelay);
                Object customerNameObject = flightDelay.get("CUSTOMERNAME");
                String customerName = customerNameObject != null ? customerNameObject instanceof Utf8 ? customerNameObject.toString() : (String) customerNameObject : "nullname";
                Object emailObject = flightDelay.get("EMAIL");
                String email = emailObject != null ? emailObject instanceof Utf8 ? emailObject.toString() : (String) emailObject : "nullemil";
                Object arrivalCodeObject = flightDelay.get("ARRIVAL_CODE");
                String arrivalCode =   arrivalCodeObject != null ? arrivalCodeObject instanceof Utf8 ? arrivalCodeObject.toString() : (String) arrivalCodeObject : "nullarrivalcode";
                Object timeDelayObject =  flightDelay.get("TIME_DELAY");
                long timeDelay = timeDelayObject != null ? (Long) timeDelayObject : 0L;
                logger.log(String.format("Received the following Customer Name:[%s], Email:[%s], Arrival Code:[%s], Delay(seconds):[%d]",
                        customerName, email,arrivalCode, timeDelay));
            });
        });
        logger.log("Done processing all flight delays");
        return null;
    }

    private byte[] decode(final String encoded) {
        return Base64.getDecoder().decode(encoded);
    }

    private <K,V> Map<K, V> getMapFromString(final String value)  {
        try {
            return objectMapper.readValue(value, new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> getSecretsConfigs() {
        String secretName = "CCloudLambdaCredentials";
        Region region = Region.of("us-west-2");
        SecretsManagerClient client = SecretsManagerClient.builder()
                .region(region)
                .build();
        String secret;
        GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();
        GetSecretValueResponse getSecretValueResponse;
        try {
            getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
            if (getSecretValueResponse.secretString() != null) {
                secret = getSecretValueResponse.secretString();
            } else {
                secret = new String(Base64.getDecoder().decode(getSecretValueResponse.secretBinary().asByteBuffer()).array());
            }
            return getMapFromString(secret);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
