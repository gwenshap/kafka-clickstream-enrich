import com.sun.tools.internal.jxc.ap.Const;
import model.PageView;
import model.Search;
import model.UserActivity;
import model.UserProfile;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import serde.JsonDeserializer;
import serde.JsonSerializer;
import serde.WrapperSerde;

import java.util.Properties;

/**
 * Created by gwen on 1/29/17.
 */
public class ClickstreamEnrichment {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        // Since each step in the stream will involve different objects, we can't use default Serde

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // this was resolved in 0.10.2.0 and above
        // don't use in large production apps - this increases network load
        // props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<Integer, PageView> views = builder.stream(Serdes.Integer(), new PageViewSerde(), Constants.PAGE_VIEW_TOPIC);
        KTable<Integer, UserProfile> profiles = builder.table(Serdes.Integer(), new ProfileSerde(), Constants.USER_PROFILE_TOPIC, "profile-store");
        KStream<Integer, Search> searches = builder.stream(Serdes.Integer(), new SearchSerde(), Constants.SEARCH_TOPIC);

        KStream<Integer, UserActivity> viewsWithProfile = views.leftJoin(profiles,
                    (page, profile) -> new UserActivity(
                        profile.getUserID(), profile.getUserName(), profile.getZipcode(), profile.getInterests(), "", page.getPage()));
        //,Serdes.Integer(), new UserActivitySerde()

        KStream<Integer, UserActivity> userActivityKStream = viewsWithProfile.leftJoin(searches,
                (userActivity, search) -> userActivity.updateSearch(search.getSearchTerms()),
                JoinWindows.of(1000), Serdes.Integer(), new UserActivitySerde(), new SearchSerde());

        userActivityKStream.to(Serdes.Integer(), new UserActivitySerde(), Constants.USER_ACTIVITY_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, props);

        streams.cleanUp();

        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(60000L);

        streams.close();


    }

    static public final class PageViewSerde extends WrapperSerde<PageView> {
        public PageViewSerde() {
            super(new JsonSerializer<PageView>(), new JsonDeserializer<PageView>(PageView.class));
        }
    }

    static public final class ProfileSerde extends WrapperSerde<UserProfile> {
        public ProfileSerde() {
            super(new JsonSerializer<UserProfile>(), new JsonDeserializer<UserProfile>(UserProfile.class));
        }
    }

    static public final class SearchSerde extends WrapperSerde<Search> {
        public SearchSerde() {
            super(new JsonSerializer<Search>(), new JsonDeserializer<Search>(Search.class));
        }
    }

    static public final class UserActivitySerde extends WrapperSerde<UserActivity> {
        public UserActivitySerde() {
            super(new JsonSerializer<UserActivity>(), new JsonDeserializer<UserActivity>(UserActivity.class));
        }
    }
}
