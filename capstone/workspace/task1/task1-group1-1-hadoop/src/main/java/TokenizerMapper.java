import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    public static final String SEPARATORS = ",\n";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        for (String eachCode : codes(value.toString())) {
            context.write(new Text(eachCode), new IntWritable(1));
        }
    }

    List<String> codes(String value) {
        List<String> result = new LinkedList<>();
        StringTokenizer tokenizer = new StringTokenizer(value, SEPARATORS);
        while (tokenizer.hasMoreElements()) {
            result.add((String) tokenizer.nextElement());
        }
        return result;
    }
}
