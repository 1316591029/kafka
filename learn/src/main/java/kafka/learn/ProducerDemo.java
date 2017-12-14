package kafka.learn;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 文件描述:
 */

public class ProducerDemo {
    public static void main(String[] args) {
        boolean isAsync = args.length == 0 ||
                /* 消息的发送方式：异步发送还是同步发送 */
                !args[0].trim().equalsIgnoreCase("sync");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        /* 客户端的 ID */
        props.put("client.id", "DemoProducer");
        /*
        * 消息的 key 和 value 都是字节数组，为了将 Java 对象转化为字节数组，可以配置
        * "key.serializer" 和 "value.serializer" 两个序列化器，完成转化
        */
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        /* StringSerializer 用来将 String 对象序列化成字节数组 */
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /* 生产者的核心类 */
        KafkaProducer producer = new KafkaProducer<>(props);

        /* 向指定的 test 这个 topic 发送消息 */
        String topic = "test";

        /* 消息的 key */
        int messageNo = 1;

        while (true) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();

            if (isAsync) { /* 异步发送消息 */
                /*
                *  第一个参数是 ProducerRecord 类型的对象，封装了目标 Topic，消息的 kv
                *  第二个参数是一个 CallBack 对象，当生产者接收到 Kafka 发来的 ACK 确认消息的时候，
                *  会调用此 CallBack 对象的 onCompletion() 方法，实现回调功能
                */
                producer.send(new ProducerRecord<>(topic, messageNo, messageStr),
                        new DemoCallBack(startTime, messageNo, messageStr));
            } else { /* 同步发送消息 */
                try {
                    /*
                    * KafkaProducer.send() 方法的返回值类型是 Future<RecordMetadata>
                    * 这里通过 Future.get 方法，阻塞当前线程，等待 Kafka 服务端的 ACK 响应
                    */
                    producer.send(new ProducerRecord<>(topic, messageNo, messageStr)).get();
                    System.out.printf("Send message: (%d, %s)\n", messageNo, messageStr);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            /* 递增消息的 key */
            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {
    /* 开始发送消息的时间戳 */
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * 生产者成功发送消息，收到 Kafka 服务端发来的 ACK 确认消息后，会调用此回调函数
     * @param metadata 生产者发送的消息的元数据，如果发送过程中出现异常，此参数为 null
     * @param exception 发送过程中出现的异常，如果发送成功为 null
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.printf("message: (%d, %s) send to partition %d, offset: %d, in %d\n",
                    key, message, metadata.partition(), metadata.offset(), elapsedTime);
        } else {
            exception.printStackTrace();
        }
    }
}
