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
public class TopPopularAirportAll {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = getInstance(conf, "Top popular airports");

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        addInputPath(job, new Path(args[0]));
        job.setNumReduceTasks(20);

        job.setJarByClass(TopPopularAirportAll.class);
        exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
