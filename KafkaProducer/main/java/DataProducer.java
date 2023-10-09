import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class DataProducer {
    private Producer<String, String> producer;
    private String traceFileName;

    public DataProducer(Producer producer, String traceFileName) {
        this.producer = producer;
        this.traceFileName = traceFileName;
    }

    /**
      Task 1:
        In Task 1, you need to read the content in the tracefile we give to you, 
        create two streams, and feed the messages in the tracefile to different 
        streams based on the value of "type" field in the JSON string.

        Please note that you're working on an ec2 instance, but the streams should
        be sent to your samza cluster. Make sure you can consume the topics on the
        master node of your samza cluster before you make a submission.
    */
    public void sendData() {
        BufferedReader reader;
        Gson gson = new Gson();
        try {
            reader = new BufferedReader(new FileReader(traceFileName));
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                JsonObject obj = gson.fromJson(line, JsonObject.class);
                int partition = obj.get("blockId").getAsInt() % 5;
                String topic = obj.get("type").getAsString().equals("DRIVER_LOCATION") ? "driver-locations" : "events";
                producer.send(new ProducerRecord<>(topic, partition, null, line));
            }
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        producer.close();
        System.out.println("Stream Created.");
    }

}
