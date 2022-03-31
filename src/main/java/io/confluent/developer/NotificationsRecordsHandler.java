package io.confluent.developer;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.RawMessage;
import com.amazonaws.services.simpleemail.model.SendRawEmailRequest;
import com.amazonaws.services.simpleemail.model.SendRawEmailResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NotificationsRecordsHandler implements RequestHandler<Map<String, Object>, Void> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Object> configs = new HashMap<>();
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final AmazonSimpleEmailService emailClient;
    private static final String SUBJECT = "Possible Flight Delays";
    private static final String BODY_TEXT_TEMPLATE = "Hi NAME, \r\n Theres a possible flight delay";
    private static String BODY_HTML_TEMPLATE = "<html>"
            + "<head></head>"
            + "<body>"
            + "<h1>Hello NAME</h1>"
            + "<p>Theres a possible delay of TIME</p>"
            + "</body>"
            + "</html>";
    private final String sender;
    private final String receivers;
    public NotificationsRecordsHandler() {
        configs.putAll(getSecretsConfigs());
        sender = (String) configs.remove("sender");
        String rawReceivers = (String) configs.remove("receivers");
        receivers = rawReceivers != null ? rawReceivers : "";
        configs.put("security.protocol", "SASL_SSL");
        configs.put("sasl.mechanism", "PLAIN");
        configs.put("basic.auth.credentials.source", "USER_INFO");
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "LambdaProducer");
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        stringDeserializer.configure(configs, false);
        emailClient = AmazonSimpleEmailServiceClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void handleRequest(Map<String, Object> payload, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Configured receivers are " + receivers);
        logger.log("Configured sender(s)" + sender);
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
                Map<String, Object> flightDelay = getMapFromString(stringDeserializer.deserialize("", bytes));
                logger.log("Record key is " + callSign + " Record value is " + flightDelay);
                Object customerNameObject = flightDelay.get("CUSTOMERNAME");
                String customerName = customerNameObject != null ?  (String) customerNameObject : "nullname";
                Object emailObject = flightDelay.get("EMAIL");
                String email = emailObject != null ? (String) emailObject : "nullemil";
                Object arrivalCodeObject = flightDelay.get("ARRIVAL_CODE");
                String arrivalCode =   arrivalCodeObject != null ? (String) arrivalCodeObject : "nullarrivalcode";
                Object timeDelayObject =  flightDelay.get("TIME_DELAY");
                int timeDelay = timeDelayObject != null ? (Integer) timeDelayObject : 0;
                logger.log(String.format("Received the following JSON notification - Customer Name:[%s], Email:[%s], Arrival Code:[%s], Delay(seconds):[%d]",
                        customerName, email,arrivalCode, timeDelay));
            });
            try {
                Session session = Session.getDefaultInstance(new Properties());
                MimeMessage mainOuterMessage = new MimeMessage(session);
                mainOuterMessage.setSubject(SUBJECT, "UTF-8");
                mainOuterMessage.setFrom(new InternetAddress(sender));
                mainOuterMessage.setRecipients(RecipientType.TO, receivers);

                logger.log("Recipients set " + Arrays.toString(mainOuterMessage.getRecipients(RecipientType.TO)));
                MimeMultipart messageBody = new MimeMultipart("alternative");
                MimeBodyPart wrapper = new MimeBodyPart();

                MimeBodyPart textPart = new MimeBodyPart();
                textPart.setContent(BODY_TEXT_TEMPLATE, "text/plain; charset=UTF-8");

                MimeBodyPart htmlPart = new MimeBodyPart();
                htmlPart.setContent(BODY_HTML_TEMPLATE, "text/html; charset=UTF-8");

                messageBody.addBodyPart(textPart);
                messageBody.addBodyPart(htmlPart);
                wrapper.setContent(messageBody);

                MimeMultipart mimeMultipart = new MimeMultipart("mixed");
                mimeMultipart.addBodyPart(wrapper);
                mainOuterMessage.setContent(mimeMultipart);

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                mainOuterMessage.writeTo(baos);
                logger.log("View the raw email " + baos);
                mainOuterMessage.writeTo(baos);

                RawMessage rawMessage = new RawMessage(ByteBuffer.wrap(baos.toByteArray()));
                SendRawEmailRequest sendRawEmailRequest = new SendRawEmailRequest(rawMessage);
                SendRawEmailResult rawEmailResult = emailClient.sendRawEmail(sendRawEmailRequest);
                logger.log("Sent an email! send result " + rawEmailResult);


            } catch (MessagingException | IOException e) {
                logger.log("Error sending email message " +e.getMessage());
                throw new RuntimeException(e);
            }
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
