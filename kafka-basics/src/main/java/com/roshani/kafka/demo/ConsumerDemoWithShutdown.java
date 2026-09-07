    package com.roshani.kafka.demo;

    import org.apache.kafka.clients.consumer.ConsumerRecord;
    import org.apache.kafka.clients.consumer.ConsumerRecords;
    import org.apache.kafka.clients.consumer.KafkaConsumer;
    import org.apache.kafka.common.errors.WakeupException;
    import org.apache.kafka.common.serialization.StringDeserializer;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.time.Duration;
    import java.util.Arrays;
    import java.util.Properties;

    public class ConsumerDemoWithShutdown {

        private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

        public static void main(String[] args) {
            log.info("I am a Kafka Consumer");

            // to create a new consumer group
            String groupId = "my-java-application";

            String topic = "demo-java";

            // create consumer properties
            Properties properties = new Properties();

            // connect to localhost
            properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

            // set consumer configs
            properties.setProperty("key.deserializer", StringDeserializer.class.getName());
            properties.setProperty("value.deserializer", StringDeserializer.class.getName());

            properties.setProperty("group.id", groupId);

            // none - if we don't have any existing consumer group, then we fail. so must set Consumer group before starting the application
            // earliest - read from the beginning. corresponds to --from-beginning
            // latest - read from just now
            properties.setProperty("auto.offset.reset", "earliest");

            // create a consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

            // get a reference to the main thread
            // reference to the thread running in the program
            final Thread mainThread = Thread.currentThread();

            // adding the shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");

                // Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
                // trigger an exception in the consumer
                consumer.wakeup();

                // join the main thread to allow execution of the code in the main thread
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            try {
                // subscribe to a topic
                consumer.subscribe(Arrays.asList(topic));

                // poll for data
                while (true) { // infinitely poll data from topics

                    log.info("Polling...");

                    // Duration.ofMillis(1000) - waiting upto 1 second for data
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Key: " + record.key() + ", Value: " + record.value());
                        log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }

                }
            } catch (WakeupException e) {
                log.info("Consumer is starting to shut down");
            } catch (Exception e) {
                log.error("Unexpected exception in the consumer", e);
            } finally {
                consumer.close(); // close the consumer, this will also commit offsets
                log.info("The consumer is now gracefully shut down");
            }
        }
    }