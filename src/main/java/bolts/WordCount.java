package bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.stream.Collectors;

public class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    long emitIter = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        count++;
        counts.put(word, count);

        emitIter++;
        if(emitIter % 500 == 0){
            for(String top: getGivenAmountOfBiggestValuesWithKeysAsStringFromGivenHashmap(counts, 5))
            collector.emit(new Values(word, top));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    private static Integer getBiggestIntegerValueFromGivenHashmap(Map<String, Integer> counts) {
        Map<String, Integer> sortedMap = sortByValue(counts);
        Set<String> keysetFromSortedMap = sortedMap.keySet();
        Optional<String> firstKey = keysetFromSortedMap.stream().findFirst();
        String firstKeyString = firstKey.get();
        Integer biggestIntegerValue = sortedMap.get(firstKeyString);
        return biggestIntegerValue;
    }

    private static String[] getGivenAmountOfBiggestValuesWithKeysAsStringFromGivenHashmap(Map<String, Integer> counts,
                                                                                         int numberOfBiggestValues) {
        String[] arrayToReturn = new String[numberOfBiggestValues];
        List<String> givenAmountOfValuesWithKeys = new ArrayList<String>();
        Map<String, Integer> sortedMap = sortByValue(counts);
        Set<String> keysetFromSortedMap = sortedMap.keySet();
        Iterator<String> keysetIterator = keysetFromSortedMap.stream().iterator();

        while (keysetIterator.hasNext()) {
            if (numberOfBiggestValues > 0) {
                String currentKey = keysetIterator.next();
                String currentValueWithKey = "word " + currentKey + " occurs : " + sortedMap.get(currentKey) + " times";
                givenAmountOfValuesWithKeys.add(currentValueWithKey);
                numberOfBiggestValues -= 1;
            } else {
                break;
            }
        }
        for (int i = 0; i < givenAmountOfValuesWithKeys.size(); i++) {
            arrayToReturn[i] = givenAmountOfValuesWithKeys.get(i);
        }
        return arrayToReturn;
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        return map.entrySet().stream().sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }
}
