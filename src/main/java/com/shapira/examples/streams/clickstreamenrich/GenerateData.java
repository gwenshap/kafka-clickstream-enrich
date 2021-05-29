package com.shapira.examples.streams.clickstreamenrich;

import com.google.gson.Gson;
import com.shapira.examples.streams.clickstreamenrich.model.PageView;
import com.shapira.examples.streams.clickstreamenrich.model.Search;
import com.shapira.examples.streams.clickstreamenrich.model.UserProfile;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This class will generate fake clicks, fake searches and fake profile updates
 * For simplicity, we will actually generate very few events - 2 profiles, update to one profile, 3 searches, 5 clicks
 */
public class GenerateData {

    public static KafkaProducer<Integer, String> producer = null;

    public static void main(String[] args) throws Exception {

        System.out.println("Press CTRL-C to stop generating data");

        List<ProducerRecord<Integer, String>> records = new ArrayList<>();
        Gson gson = new Gson();


        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
                System.out.println("Shutting Down");
                if (producer != null)
                    producer.close();
        }));



        // Two users
        String[] interests1 = {"Surfing", "Hiking"};
        UserProfile user1 = new UserProfile(1, "Matthias", "94301", interests1 );

        String[] interests2 = {"Ski", "Dancing"};
        UserProfile user2 = new UserProfile(2, "Anna", "94302", interests2);

        records.add(new ProducerRecord<>(Constants.USER_PROFILE_TOPIC, user1.getUserID(), gson.toJson(user1)));
        records.add(new ProducerRecord<>(Constants.USER_PROFILE_TOPIC, user2.getUserID(), gson.toJson(user2)));

        // profile update

        String[] newInterests = {"Ski", "stream processing"};

        records.add(new ProducerRecord<>(Constants.USER_PROFILE_TOPIC, user2.getUserID(), gson.toJson(user2.update("94303", newInterests))));


        // Two searches

        Search search1 = new Search(1, "retro wetsuit");
        Search search2 = new Search(2, "light jacket");


        records.add(new ProducerRecord<>(Constants.SEARCH_TOPIC, search1.getUserID(), gson.toJson(search1)));
        records.add(new ProducerRecord<>(Constants.SEARCH_TOPIC, search2.getUserID(), gson.toJson(search2)));

        // three clicks

        PageView view1 = new PageView(1, "collections/mens-wetsuits/products/w3-worlds-warmest-wetsuit");
        PageView view2 = new PageView(2, "product/womens-dirt-craft-bike-mountain-biking-jacket");
        PageView view3 = new PageView(2, "/product/womens-ultralight-down-jacket");

        records.add(new ProducerRecord<>(Constants.PAGE_VIEW_TOPIC, view1.getUserID(), gson.toJson(view1)));
        records.add(new ProducerRecord<>(Constants.PAGE_VIEW_TOPIC, view2.getUserID(), gson.toJson(view2)));
        records.add(new ProducerRecord<>(Constants.PAGE_VIEW_TOPIC, view3.getUserID(), gson.toJson(view3)));


        // Configure a producer.
        // We'll use User ID as the key for all events - since joins require a common key
        // Since we are going to write objects of different types as values, we'll serialize all of them to JSON strings ourselves
        // So the producer type and serializer are just for strings



        Properties props = new Properties();

        props.put("bootstrap.servers", Constants.BROKER);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Starting producer
        producer = new KafkaProducer<>(props);

        // Send existing events

        for (ProducerRecord<Integer, String> record: records)
            producer.send(record, (RecordMetadata r, Exception e) -> {
                if (e != null) {
                    System.out.println("Error producing to topic " + r.topic());
                    e.printStackTrace();
                }
            });


        // Sleep 5 seconds, to make sure we recognize the new events as a separate session
        records.clear();
        Thread.sleep(5000);

        // One more search

        Search search3 = new Search(2, "carbon ski boots");

        records.add(new ProducerRecord<>(Constants.SEARCH_TOPIC, search3.getUserID(), gson.toJson(search3)));

        // Two clicks
        PageView view4 = new PageView(2, "product/salomon-quest-access-custom-heat-ski-boots-womens");
        PageView view5 = new PageView(2, "product/nordica-nxt-75-ski-boots-womens");

        records.add(new ProducerRecord<>(Constants.PAGE_VIEW_TOPIC, view4.getUserID(), gson.toJson(view4)));
        records.add(new ProducerRecord<>(Constants.PAGE_VIEW_TOPIC, view5.getUserID(), gson.toJson(view5)));

        // Click for an unknown user without searches - we want to make sure we have results for those too.

        PageView view6 = new PageView(-1, "product/osprey-atmos-65-ag-pack");
        records.add(new ProducerRecord<>(Constants.PAGE_VIEW_TOPIC, view6.getUserID(), gson.toJson(view6)));


        // Send additional events
        for (ProducerRecord<Integer, String> record: records)
            producer.send(record, (RecordMetadata r, Exception e) -> {
                if (e != null) {
                    System.out.println("Error producing to topic " + r.topic());
                    e.printStackTrace();
                }
            });


        // and done...

        producer.close();


    }


}
