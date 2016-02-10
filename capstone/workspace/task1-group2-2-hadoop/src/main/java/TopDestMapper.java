import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 28/01/16.
 */
public class TopDestMapper extends Mapper<Object, Text, Text, TextArrayWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\t");
        String originCarrierKey = split[0];
        String origin = origin(originCarrierKey);
        String dest = dest(originCarrierKey);
        context.write(new Text(origin), new TextArrayWritable(new String[]{dest, split[1]}));
    }

    private String dest(String s) {
        return s.split("-")[1];
    }

    private String origin(String s) {
        return s.split("-")[0];
    }
}
