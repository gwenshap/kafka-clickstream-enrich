# kafka-clickstream-enrich
Kafka Streams Example - Joining streams to generate rich clickstream analysis data

Overview
--------
This example takes 2 streams of data: Stream of searches and stream of user clicks
It also takes a stream of profile-updates, representing changes to a profiles table (assume we are getting those changes from MySQL using KafkaConnect connector)

It joins those activity streams together, to generate an holistic view of user activity. The results show you, in one record the user's location, interests, what they searched for and what they ended up clicking. Providing rich source of data for analysis - which products are more desirable for which audience? "users who searched for this also looked at..." and other data products.

This example makes use of the unique windowed-join, allowing us to match clicks with the search that happened in the same time window - in order to generate relevant results.

To run:
--------

0. Build the project with `mvn package`, this will generate an uber-jar with the streams app and all its dependencies.
1. Next, we need to generate some clicks, searches and profiles. Run the generator. It should take about 5 seconds to run. Don't worry about complete lack of output... 
   `$ java -cp target/uber-kafka-clickstream-enrich-1.0-SNAPSHOT.jar com.shapira.examples.streams.clickstreamenrich.GenerateData`
2. Run the streams app:
   `java -cp target/uber-kafka-clickstream-enrich-1.0-SNAPSHOT.jar com.shapira.examples.streams.clickstreamenrich.ClickstreamEnrichment`
   Streams apps typically run forever, but this one will just run for a minute and exit
3. Check the results:
   `bin/kafka-console-consumer.sh --topic clicks.user.activity --from-beginning --bootstrap-server localhost:9092  --property print.key=true`

If you want to reset state and re-run the application (maybe with some changes?) on existing input topic, you can:

1. Reset internal topics (used for shuffle and state-stores):

    `bin/kafka-streams-application-reset.sh --application-id clicks --bootstrap-servers localhost:9092 --input-topics clicks.user.profile,clicks.pages.views,clicks.search `

2. (optional) Delete the output topic:

    `bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic clicks.user.activity`