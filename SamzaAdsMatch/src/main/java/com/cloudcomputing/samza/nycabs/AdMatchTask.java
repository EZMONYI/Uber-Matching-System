package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.NoSuchElementException;


/**
 * Consumes the stream of events.
 * Outputs a stream which handles static file and one stream
 * and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */

    private KeyValueStore<Integer, Map<String, Object>> userInfo;

    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    private Set<String> lowCalories;

    private Set<String> energyProviders;

    private Set<String> willingTour;

    private Set<String> stressRelease;

    private Set<String> happyChoice;

    private void initSets() {
        lowCalories = new HashSet<>(Arrays.asList("seafood", "vegetarian", "vegan", "sushi"));
        energyProviders = new HashSet<>(Arrays.asList("bakeries", "ramen", "donuts", "burgers",
                "bagels", "pizza", "sandwiches", "icecream",
                "desserts", "bbq", "dimsum", "steak"));
        willingTour = new HashSet<>(Arrays.asList("parks", "museums", "newamerican", "landmarks"));
        stressRelease = new HashSet<>(Arrays.asList("coffee", "bars", "wine_bars", "cocktailbars", "lounges"));
        happyChoice = new HashSet<>(Arrays.asList("italian", "thai", "cuban", "japanese", "mideastern",
                "cajun", "tapas", "breakfast_brunch", "korean", "mediterranean",
                "vietnamese", "indpak", "southern", "latin", "greek", "mexican",
                "asianfusion", "spanish", "chinese"));
    }

    // Get store tag
    private String getTag(String cate) {
        String tag = "";
        if (happyChoice.contains(cate)) {
            tag = "happyChoice";
        } else if (stressRelease.contains(cate)) {
            tag = "stressRelease";
        } else if (willingTour.contains(cate)) {
            tag = "willingTour";
        } else if (energyProviders.contains(cate)) {
            tag = "energyProviders";
        } else if (lowCalories.contains(cate)) {
            tag = "lowCalories";
        } else {
            tag = "others";
        }
        return tag;
    }

    private int getValueScore(String device, String price) {
        int priceScore = 0;
        int deviceScore = 0;
        if (price.equals("$$$") || price.equals("$$$$")) {
            priceScore = 3;
        } else if (price.equals("$$")) {
            priceScore = 2;
        } else if (price.equals("$")) {
            priceScore = 1;
        } else {
            priceScore = 0;
        }

        if (device.equals("iPhone XS")) {
            deviceScore = 3;
        } else if (device.equals("iPhone 7")) {
            deviceScore = 2;
        } else if (device.equals("iPhone 5")) {
            deviceScore = 1;
        } else {
            deviceScore = 0;
        }

        return Math.abs(priceScore - deviceScore);
    }


    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        } else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            return dist;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize kv store

        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getTaskContext().getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("yelp-info");

        //Initialize store tags set
        initSets();

        //Initialize static data and save them in kv store
        initialize("UserInfoData.json", "NYCstore.json");
    }

    /**
     * This function will read the static data from resources folder
     * and save data in KV store.
     * <p>
     * This is just an example, feel free to change them.
     */
    public void initialize(String userInfoFile, String businessFile) {
        List<String> userInfoRawString = AdMatchConfig.readFile(userInfoFile);
        System.out.println("Reading user info file from " + Resources.getResource(userInfoFile).toString());
        System.out.println("UserInfo raw string size: " + userInfoRawString.size());
        for (String rawString : userInfoRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                int userId = (Integer) mapResult.get("userId");
                userInfo.put(userId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse user info :" + rawString);
            }
        }

        List<String> businessRawString = AdMatchConfig.readFile(businessFile);

        System.out.println("Reading store info file from " + Resources.getResource(businessFile).toString());
        System.out.println("Store raw string size: " + businessRawString.size());

        for (String rawString : businessRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                String storeId = (String) mapResult.get("storeId");
                String cate = (String) mapResult.get("categories");
                String tag = getTag(cate);
                mapResult.put("tag", tag);
                yelpInfo.put(storeId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse store info :" + rawString);
            }
        }
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

        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
            processevent((Map<String, Object>)envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
    public void processevent(Map<String, Object> message, MessageCollector collector) {
        if (message.get("type").equals("RIDER_STATUS")) {
            int userId = (int)message.get("userId");
            int mood = (int)message.get("mood");
            int bloodSugar = (int)message.get("blood_sugar");
            int stress = (int)message.get("stress");
            int active = (int)message.get("active");

            Map<String, Object> update = userInfo.get(userId);
            update.put("mood", mood);
            update.put("blood_sugar", bloodSugar);
            update.put("stress", stress);
            update.put("active", active);
            userInfo.put(userId,update);

        } else if (message.get("type").equals("RIDER_INTEREST")) {
            if ((int)message.get("duration") > 300000) {
                int userId = (int)message.get("userId");
                Map<String, Object> update = userInfo.get(userId);
                update.put("interest", message.get("interest") + "");
                userInfo.put(userId, update);
            }
        } else if (message.get("type").equals("RIDE_REQUEST")) {
            int userId = (int)message.get("clientId");
            Map<String, Object> currUserInfo = userInfo.get(userId);
            List<String> userTags = new ArrayList<>();
            int mood = (int)currUserInfo.get("mood");
            int bloodSugar = (int)currUserInfo.get("blood_sugar");
            int stress = (int)currUserInfo.get("stress");
            int active = (int)currUserInfo.get("active");

            if (bloodSugar > 4 && mood > 6 && active == 3) userTags.add("lowCalories");
            if (bloodSugar < 2 || mood < 4) userTags.add("energyProviders");
            if (active == 3) userTags.add("willingTour");
            if (stress > 5 || active == 1 || mood < 4) userTags.add("stressRelease");
            if (mood > 6) userTags.add("happyChoice");
            if (userTags.isEmpty()) userTags.add("others");


            KeyValueIterator<String, Map<String, Object>> stores = yelpInfo.all();
            List<Entry<String, Map<String, Object>>> rightStores = new ArrayList<>();
            try {
                while (stores.hasNext()) {
                    Entry<String, Map<String, Object>> e = stores.next();
                    Map<String, Object> evalue = e.getValue();
                    if (userTags.contains(evalue.get("tag") + "")) rightStores.add(e);
                }
            } catch (NoSuchElementException e) {

            }

            stores.close();

            String storeId = "";
            String storeName = "";
            double maxScore = 0;

            for (int i = 0; i < rightStores.size(); i++) {
                double score = 0;
                Entry<String, Map<String, Object>> e = rightStores.get(i);
                String ekey = e.getKey();
                Map<String, Object> evalue = e.getValue();
                score = (int) evalue.get("review_count") * (double) evalue.get("rating");
                if ((evalue.get("categories") + "").equals((currUserInfo.get("interest") + ""))) score += 10;
                score = score * (1 - getValueScore(currUserInfo.get("device") + "", evalue.get("price") + "") * 0.1);

                double userLatitude = (double) message.get("latitude");
                double userLongitude = (double) message.get("longitude");
                double storeLatitude = (double) evalue.get("latitude");
                double storeLongitude = (double) evalue.get("longitude");
                double currDistance = distance(userLatitude, userLongitude, storeLatitude, storeLongitude);

                if ((int) currUserInfo.get("age") == 20 || (int) currUserInfo.get("travel_count") > 50) {
                    if (currDistance > 10) score = score * 0.1;
                } else {
                    if (currDistance > 5) score = score * 0.1;
                }

                if (score > maxScore) {
                    maxScore = score;
                    storeId = ekey;
                    storeName = evalue.get("name") + "";
                }
            }

            HashMap<String,Object> match = new HashMap<>();
            if ((!storeId.equals("")) && !storeName.equals("")) {
                match.put("userId", userId);
                match.put("storeId", storeId);
                match.put("name", storeName);
                collector.send(new OutgoingMessageEnvelope(AdMatchConfig.AD_STREAM, null, null, match));
            }

        }
    }
}

