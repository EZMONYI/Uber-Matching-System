package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.HashMap;


/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

    /* Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */
    private KeyValueStore<String, String> driverloc;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe the kv stores?)
        driverloc = (KeyValueStore<String, String>)context.getTaskContext().getStore("driver-loc");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            processdriverlocations((Map<String, Object>) envelope.getMessage(), collector);
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            processevent((Map<String, Object>)envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    public void processdriverlocations(Map<String, Object> message, MessageCollector collector) {
        //if it is not location file  throw exception
        if (!message.get("type").equals("DRIVER_LOCATION")) {
            throw new IllegalStateException("Unexpected event type on follows stream: " + message.get("event"));
        }

        String driverId = (int) message.get("driverId") + "";
        String block = (int)message.get("blockId") + "";
        String latitude = (double) message.get("latitude") + "";
        String longitude = (double) message.get("longitude") + "";

        driverloc.put(block + ":" + driverId, latitude + ":" + longitude);

    }

    public void processevent(Map<String, Object> message, MessageCollector collector) {
        //get the driver's event
        if (message.get("type").equals("ENTERING_BLOCK")) {
            String driverId = (int) message.get("driverId") + "";
            String block = (int)message.get("blockId") + "";

            String latitude = (double) message.get("latitude") + "";
            String longitude = (double) message.get("longitude") + "";
            String gender = message.get("gender") + "";
            String rating = (double) message.get("rating") + "";
            String salary = (int) message.get("salary") + "";

            //if availabe put in store
            if (message.get("status").equals("AVAILABLE")) {
                driverloc.put(block + ":" + driverId, latitude + ":" + longitude + ":" + gender + ":" + rating + ":" + salary);
            } else {
                //if not just delete it
                driverloc.delete(block + ":" + driverId);
            }
            //if it is RIDE_COMP put back to store
        } else if (message.get("type").equals("LEAVING_BLOCK")) {
            String driverId = (int) message.get("driverId") + "";
            String block = (int)message.get("blockId") + "";
            driverloc.delete(block + ":" + driverId);
        } else if (message.get("type").equals("RIDE_COMPLETE")) {
            String driverId = (int) message.get("driverId") + "";
            String block = (int)message.get("blockId") + "";

            String latitude = (double) message.get("latitude") + "";
            String longitude = (double) message.get("longitude") + "";
            String gender = message.get("gender") + "";
            double oldRating = (double) message.get("rating");
            String salary = (int) message.get("salary") + "";

            double userRating = (double) message.get("user_rating");
            String rating = ((oldRating + userRating) / 2) + "";

            driverloc.put(block + ":" + driverId, latitude + ":" + longitude + ":" + gender + ":" + rating + ":" + salary);

        } else if (message.get("type").equals("RIDE_REQUEST")) {

            String clientId = (int) message.get("clientId") + "";
            String clientBlock = (int) message.get("blockId") + "";

            double clientLatitude = (double) message.get("latitude");
            double clientLongitude = (double) message.get("longitude");
            String genderPref = message.get("gender_preference") + "";


            KeyValueIterator<String, String> drivers = driverloc.range(clientBlock + ":", clientBlock + ";");
            String driverId = "";
            String key = "";
            double maxScore = 0;
            try {
                while (drivers.hasNext()) {
                    Entry<String, String> e = drivers.next();
                    String ekey = e.getKey();
                    String evalue = e.getValue();
                    String[] driver = evalue.split(":");
                    if ((driver.length == 5)) {
                        double driverLatitude = Double.parseDouble(driver[0]);
                        double driverLongtitude = Double.parseDouble(driver[1]);
                        String gender = driver[2];
                        double rating = Double.parseDouble(driver[3]);
                        int salary = Integer.parseInt(driver[4]);
                        double distanceScore = 1 * Math.exp(-1 * Math.sqrt((int)Math.pow((driverLatitude - clientLatitude), 2) 
                                             + (int)Math.pow((driverLongtitude - clientLongitude), 2)));
                        double genderScore;
                        if (genderPref == "N") genderScore = 0.0;
                        else genderScore = genderPref.equals(gender) ? 1.0 : 0.0;
                        double ratingScore = rating / 5.0;
                        double salaryScore = 1 - (salary / 100.0);
                        double currentScore = distanceScore * 0.4 + genderScore * 0.1 + ratingScore * 0.3 + salaryScore * 0.2;

                        if (currentScore > maxScore) {
                            maxScore = currentScore;
                            driverId = ekey.split(":")[1];
                            key = ekey;
                        }
                    }
                }
            } catch (NoSuchElementException e) {

            }
            drivers.close();

            //finish scan and send out driverid and riderid
            HashMap<String,Object> match = new HashMap<>();
            if ((!driverId.equals("")) && (!key.equals(""))) {
                match.put("driverId", Integer.parseInt(driverId));
                match.put("clientId", Integer.parseInt(clientId));

                driverloc.delete(key);
                collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, null, null, match));
            }

        }
    }
}
