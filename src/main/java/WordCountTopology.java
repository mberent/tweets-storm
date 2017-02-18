import bolts.TweetHashtagsDeserialize;
import bolts.TweetTextDeserialize;
import bolts.WordCount;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.starter.spout.RandomSentenceSpout;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {

    private static final String KAFKA_BROKER_PROPERTIES = "kafka.broker.properties";

    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpout tweetTextSpout = prepareKafkaSpout("tweets-text");
        builder.setSpout("spout", tweetTextSpout, 5);
        builder.setBolt("deserialize", new TweetTextDeserialize(), 5)
                .shuffleGrouping("spout");

        builder.setBolt("split", new SplitSentence(), 8)
                .shuffleGrouping("deserialize");

        builder.setBolt("count", new WordCount(), 1)
                .fieldsGrouping("split", new Fields("word"));

        KafkaBolt tweetTextResult = new KafkaBolt()
                .withProducerProperties(prepareKafkaProducerProperties())
                .withTopicSelector(new DefaultTopicSelector("result"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
        builder.setBolt("forwardToKafka", tweetTextResult, 8).shuffleGrouping("count");


        KafkaSpout tweetHashtagsSpout = prepareKafkaSpout("tweets-hashtags");
        builder.setSpout("tweetHashtagsSpout", tweetHashtagsSpout, 5);
        builder.setBolt("deserialize_hashtags", new TweetHashtagsDeserialize(), 5)
                .shuffleGrouping("tweetHashtagsSpout");

        builder.setBolt("split_hashtags", new SplitSentence(), 8)
                .shuffleGrouping("deserialize_hashtags");

        builder.setBolt("count_hashtags", new WordCount(), 1)
                .fieldsGrouping("split_hashtags", new Fields("word"));

        KafkaBolt tweetHashtagsResult = new KafkaBolt()
                .withProducerProperties(prepareKafkaProducerProperties())
                .withTopicSelector(new DefaultTopicSelector("result-hashtags"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
        builder.setBolt("forwardToKafka_hashtags", tweetHashtagsResult, 8)
                .shuffleGrouping("count_hashtags");


        Config conf = new Config();
//        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(8);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(1000000);

            cluster.shutdown();
        }
    }

    private static KafkaSpout prepareKafkaSpout(String topic){
        String zkConnString = "localhost";

        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig spoutConfig = new SpoutConfig(
                hosts,
                topic,
                "/" + topic,
                UUID.randomUUID().toString());

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }

    private static Properties prepareKafkaProducerProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}