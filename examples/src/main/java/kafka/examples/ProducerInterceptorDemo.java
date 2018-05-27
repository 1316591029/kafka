package kafka.examples;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 文件描述:
 */

public class ProducerInterceptorDemo implements ProducerInterceptor<Integer, String> {
    Logger log = LoggerFactory.getLogger(ProducerInterceptorDemo.class);

    @Override
    public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
        // 过滤奇数
        if (record.key() % 2 == 0) return record;
        return new ProducerRecord<>(record.topic(), record.partition(), record.key(), "None");
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null && "test".equals(metadata.topic()) && metadata.partition() == 0) {
            log.info("Interceptor Demo: {}", metadata.toString());
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        // 初始化使用
    }
}
