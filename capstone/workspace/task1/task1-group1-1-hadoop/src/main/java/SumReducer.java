import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

import static org.apache.commons.collections4.CollectionUtils.reverseArray;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int maxElements = 10;

    private TreeBidiMap<Integer, String> cache = new TreeBidiMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        updateCache(sum(values), key.toString());
    }

    void updateCache(Integer sum, String code) {
        if (cache.containsValue(code)) {
            cache.removeValue(code);
        }
        cache.put(sum, code);
        if (cache.size() > maxElements) {
            cache.remove(cache.firstKey());
        }
    }

    private int sum(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        return sum;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Integer> integers = cache.keySet();
        reverseArray(integers.toArray());
        for (Integer key : integers) {
            context.write(new Text(cache.get(key)), new IntWritable(key));
        }
    }

    public void setMaxElements(int maxElements) {
        this.maxElements = maxElements;
    }

    public TreeBidiMap<Integer, String> getCache() {
        return cache;
    }
}
