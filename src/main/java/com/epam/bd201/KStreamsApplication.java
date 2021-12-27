package com.epam.bd201;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import org.json.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamsApplication {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "m12_kafka_streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("schema.registry.url", "schemaregistry:8081");

        final String INPUT_TOPIC = "expedia";
        final String OUTPUT_TOPIC = "expedia_ext";

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<byte[], String> input_records = builder.stream(INPUT_TOPIC);

        input_records.mapValues(value -> {
            JSONObject obj = new JSONObject(value);

            try {
                Date srchCi = new SimpleDateFormat("yyyy-MM-dd").parse(obj.getString("srch_ci"));
                Date srchCo = new SimpleDateFormat("yyyy-MM-dd").parse(obj.getString("srch_co"));
                long millisecondsBetween = srchCo.getTime() - srchCi.getTime();
                int daysStayed = (int)(millisecondsBetween / 1000 / 60 / 60 / 24);
                String stayCategory;

                if (daysStayed <= 0)
                {
                    stayCategory = "Erroneous data";
                } else if (daysStayed <= 4) {
                    stayCategory = "Short stay";
                } else if (daysStayed <= 10) {
                    stayCategory = "Standard stay";
                } else if (daysStayed <= 14) {
                    stayCategory = "Standard extended stay";
                }
                else {
                    stayCategory = "Long stay";
                }

                obj.put("stay_category", stayCategory);

            } catch (java.text.ParseException | org.json.JSONException e) {
                System.out.println("Not able to parse dates");
            }

            return obj.toString();
        }).to(OUTPUT_TOPIC);

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
