package bolts;

import com.google.gson.Gson;
import domains.TweetText;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TweetTextDeserialize extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Gson gson = new Gson();
        String sentence = tuple.getString(0);
        TweetText tweet = gson.fromJson(sentence, TweetText.class);
        collector.emit(new Values(tweet.getMessage()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-text"));
    }
}
