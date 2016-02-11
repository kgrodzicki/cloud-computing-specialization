import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class PerformanceReducer extends Reducer<Text, IntWritable, Text, Text> {

    public static Logger log = Logger.getLogger(PerformanceReducer.class);

    private int maxElements = 10;

    private TreeBidiMap<Double, String> cache = new TreeBidiMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long onTime = 0;
        long notOnTime = 0;
        for (IntWritable each : values) {
            if (each.get() == 1) {
                onTime++;
            } else {
                notOnTime++;
            }
        }
        double performanceOnTime = performance(onTime, notOnTime);
        log.debug("OnTime: " + onTime + ", NotOnTime " + notOnTime + ", Performance: " + performanceOnTime);
        updateCache(performanceOnTime, key.toString());
    }

    double performance(long onTime, long notOnTime) {
        if (onTime == 0) {
            return 0d;
        }
        double all = onTime + notOnTime;
        return (onTime / all) * 100;
    }

    void updateCache(double performance, String code) {
        if (cache.containsValue(code)) {
            cache.removeValue(code);
        }
        cache.put(performance, code);
        if (cache.size() > maxElements) {
            cache.remove(cache.firstKey());
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Double key : cache.keySet()) {
            context.write(new Text(cache.get(key)), new Text(key.toString()));
        }
    }

    public void setMaxElements(int maxElements) {
        this.maxElements = maxElements;
    }

    public TreeBidiMap<Double, String> getCache() {
        return cache;
    }
}
