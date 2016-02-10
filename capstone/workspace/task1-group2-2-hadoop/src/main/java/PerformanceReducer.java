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
        context.write(key, new Text(String.valueOf(performanceOnTime)));
    }

    double performance(long onTime, long notOnTime) {
        if (onTime == 0) {
            return 0d;
        }
        double all = onTime + notOnTime;
        return (onTime / all) * 100;
    }
}
