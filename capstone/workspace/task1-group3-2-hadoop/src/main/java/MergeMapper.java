import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 28/01/16.
 */
public class MergeMapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().contains("2008")) {
            context.write(value, NullWritable.get());
        }
    }

    static String[] parse(String s) {
        return s.split(TokenizerMapper.SEPARATOR);
    }
}
