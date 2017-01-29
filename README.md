# kafka-clickstream-enrich
Kafka Streams Example - Joining streams to generate rich clickstream analysis data

Overview
--------
This example takes 2 streams of data: Stream of searches and stream of user clicks
It also takes a stream of profile-updates, representing changes to a profiles table (assume we are getting those changes from MySQL using KafkaConnect connector)

It joins those activity streams together, to generate an holistic view of user activity. The results show you, in one record the user's location, interests, what they searched for and what they ended up clicking. Providing rich source of data for analysis - which products are more desirable for which audience? "users who searched for this also looked at..." and other data products.

This example makes use of the unique windowed-join, allowing us to match clicks with the search that happened in the same time window - in order to generate relevant results.

