package bolts;

import com.google.gson.Gson;
import domains.TweetHashtags;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TweetHashtagsDeserialize extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Gson gson = new Gson();
        String sentence = tuple.getString(0);
        TweetHashtags tweet = gson.fromJson(sentence, TweetHashtags.class);
        for (String hashtag : tweet.getHashtags()) {
            collector.emit(new Values(hashtag));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-hashtags"));
    }
}