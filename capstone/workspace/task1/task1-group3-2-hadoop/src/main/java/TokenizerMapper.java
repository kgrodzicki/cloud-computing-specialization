import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.tuple.ImmutablePair.of;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 28/01/16.
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, TextArrayWritable> {

    public static final String SEPARATOR = "#";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        List<ImmutablePair<String, List<String>>> pairs = parse(value.toString());
        for (ImmutablePair<String, List<String>> pair : pairs) {
            context.write(new Text(pair.left), new TextArrayWritable(pair.right.toArray(new String[pair.right.size()])));
        }
    }

    List<ImmutablePair<String, List<String>>> parse(String value) {
        List<ImmutablePair<String, List<String>>> result = newArrayList();
        StringTokenizer tokenizer = new StringTokenizer(value, "\n");
        while (tokenizer.hasMoreElements()) {
            String line = (String) tokenizer.nextElement();
            String[] parsed = line.split(",", -1);

            String origin = parsed[0];
            String dest = parsed[1];
            String flightDate = parsed[2];
            String flightNum = parsed[3];
            String depTime = parsed[4];
            String arrDelay = parsed[5];
            String cancelled = parsed[6];
            List<String> strings = newArrayList(flightNum, depTime, arrDelay);

            if (isNotCancelled(cancelled) && isNotBlank(arrDelay)) {
                result.add(of(merge(origin, dest, flightDate, DepartureType.build(Integer.valueOf(depTime)).name()), strings));
            }
        }
        return result;
    }

    private boolean isNotCancelled(String cancelled) {
        return !(isNotBlank(cancelled) && "1.00".equals(cancelled));
    }

    private String merge(String origin, String dest, String date, String departureType) {
        return origin + SEPARATOR + dest + SEPARATOR + date + SEPARATOR + departureType;
    }
}
