import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Double.valueOf;
import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    public static final String SEPARATORS = "\n";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        List<ImmutablePair<String, Integer>> pairs = codes(value.toString());
        for (ImmutablePair<String, Integer> pair : pairs) {
            context.write(new Text(pair.left), new IntWritable(pair.right));
        }
    }

    List<ImmutablePair<String, Integer>> codes(String value) {
        List<ImmutablePair<String, Integer>> result = newArrayList();
        StringTokenizer tokenizer = new StringTokenizer(value, SEPARATORS);
        while (tokenizer.hasMoreElements()) {
            String line = (String) tokenizer.nextElement();
            String[] parsed = line.split(",", -1);
            String airlineId = parsed[0];
            String delay = parsed[1];
            String isCanceled = parsed[2];
            result.add(ImmutablePair.of(airlineId, isOnTime(delay, isCanceled)));
        }
        return result;
    }

    Integer isOnTime(String delay, String isCanceled) {
        if (isCancelled(isCanceled) || isDelayed(delay)) {
            return 0;
        }
        return 1;
    }

    private boolean isDelayed(String delay) {
        return isNotBlank(delay) && valueOf(delay) > 0.;
    }

    private boolean isCancelled(String isCanceled) {
        return isNotBlank(isCanceled) && "1.00".contains(isCanceled);
    }
}
