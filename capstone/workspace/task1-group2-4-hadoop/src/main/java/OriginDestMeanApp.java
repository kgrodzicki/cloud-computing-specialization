import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import static java.lang.System.exit;
import static org.apache.hadoop.mapreduce.Job.getInstance;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class OriginDestMeanApp {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job_1 = getInstance(conf, "Origin -> Dest mean task");

        job_1.setMapperClass(TokenizerMapper.class);
        job_1.setReducerClass(MeanReducer.class);

        job_1.setMapOutputKeyClass(Text.class);
        job_1.setMapOutputValueClass(IntWritable.class);
        job_1.setOutputFormatClass(NullOutputFormat.class);

        addInputPath(job_1, new Path(args[0]));

        job_1.setJarByClass(OriginDestMeanApp.class);
        job_1.setNumReduceTasks(20);

        exit(job_1.waitForCompletion(true) ? 0 : 1);
    }
}
