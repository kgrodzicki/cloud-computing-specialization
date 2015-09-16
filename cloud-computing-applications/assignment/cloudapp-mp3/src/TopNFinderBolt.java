import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.currentTimeMillis;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
    private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();

    private int N;

    private static final long INTERVALTOREPORT = 20;

    private long lastReportTime = currentTimeMillis();

    public TopNFinderBolt(int N) {
        this.N = N;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words


    ------------------------------------------------- */
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        if (currentTopWords.size() < N) {
            currentTopWords.put(word, count);
        } else {
            removeLowest();
            currentTopWords.put(word, count);
        }

        //reports the top N words periodically
        if (currentTimeMillis() - lastReportTime >= INTERVALTOREPORT) {
            collector.emit(new Values(printMap()));
            lastReportTime = currentTimeMillis();
        }
    }

    private void removeLowest() {
        String keyWithLowestVal = null;
        Integer lowestVal = null;
        for (Map.Entry<String, Integer> each : currentTopWords.entrySet()) {
            if (lowestVal == null || each.getValue() < lowestVal) {
                keyWithLowestVal = each.getKey();
                lowestVal = each.getValue();
            }
        }
        currentTopWords.remove(keyWithLowestVal);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("top-N"));
    }

    public String printMap() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("top-words = [ ");
        for (String word : currentTopWords.keySet()) {
            stringBuilder.append("(").append(word).append(" , ").append(currentTopWords.get(word)).append(") , ");
        }
        int lastCommaIndex = stringBuilder.lastIndexOf(",");
        stringBuilder.deleteCharAt(lastCommaIndex + 1);
        stringBuilder.deleteCharAt(lastCommaIndex);
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
