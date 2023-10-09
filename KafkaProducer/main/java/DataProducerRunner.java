import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;

public class DataProducerRunner {

    public static void main(String[] args) throws Exception {
        /*
            Tasks to complete:
            - Write enough tests in the DataProducerTest.java file
            - Instantiate the Kafka Producer by following the API documentation
            - Instantiate the DataProducer using the appropriate trace file and the producer
            - Implement the sendData method as required in DataProducer
            - Call the sendData method to start sending data
        */

        Properties props = new Properties();
        props.put("bootstrap.servers", "ec2-204-236-201-73.compute-1.amazonaws.com:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        DataProducer producer = new DataProducer(new KafkaProducer<>(props), "/home/clouduser/stream-processing/DataProducer/tracefile");
        producer.sendData();
    }
}
