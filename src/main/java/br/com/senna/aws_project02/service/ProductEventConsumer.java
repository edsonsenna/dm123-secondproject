package br.com.senna.aws_project02.service;

import br.com.senna.aws_project02.model.Envelop;
import br.com.senna.aws_project02.model.ProductEvent;
import br.com.senna.aws_project02.model.ProductEventLog;
import br.com.senna.aws_project02.model.SnsMessage;
import br.com.senna.aws_project02.repository.ProductEventLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

@Service
public class ProductEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(
            ProductEventConsumer.class);

    private ObjectMapper objectMapper;
    private ProductEventLogRepository productEventLogRepository;

    @Autowired
    public ProductEventConsumer(ObjectMapper objectMapper, ProductEventLogRepository productEventLogRepository) {
        this.objectMapper = objectMapper;
        this.productEventLogRepository = productEventLogRepository;
    }

    @JmsListener(destination = "${aws.sqs.queue.product.events.name}")
    public void receiveProductEvent(TextMessage textMessage)
            throws JMSException, IOException {
        SnsMessage snsMessage = objectMapper.readValue(textMessage.getText(),
                SnsMessage.class);
        Envelop envelop = objectMapper.readValue(snsMessage.getMessage(),
                Envelop.class);
        ProductEvent productEvent = objectMapper.readValue(
                envelop.getData(), ProductEvent.class);

        ProductEventLog productEventLog = buildProductEventLog(envelop, productEvent);
        productEventLogRepository.save(productEventLog);

        log.info("Product event received - Event: {} - ProductId: {} - " +

                        "MessageId: {}", envelop.getEventType(),
                productEvent.getProductId(), snsMessage.getMessageId());

    }

    ProductEventLog buildProductEventLog(Envelop envelope,

                                         ProductEvent productEvent) {
        ProductEventLog productEventLog = new ProductEventLog();
        productEventLog.setEventType(envelope.getEventType());
        productEventLog.setProductId(productEvent.getProductId());
        productEventLog.setCode(productEvent.getCode());
        productEventLog.setUsername(productEvent.getUsername());
        productEventLog.setTimestamp(Instant.now().toEpochMilli());
        productEventLog.setTtl(Instant.now().plus(
                Duration.ofMinutes(5)).getEpochSecond());
        return productEventLog;
    }
}