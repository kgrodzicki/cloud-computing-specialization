import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import static java.lang.System.exit;
import static org.apache.hadoop.mapreduce.Job.getInstance;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class MergeFlightsXYZ {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        Path tmpPath = new Path("/tmp-3-2-2008");
        FileSystem fs = FileSystem.get(conf);
//        fs.delete(tmpPath, true);

//        Job job_1 = getInstance(conf, "Persist best flights");
//
//        job_1.setMapperClass(TokenizerMapper.class);
//        job_1.setReducerClass(FlightInfoReducer.class);
//
//        job_1.setMapOutputKeyClass(Text.class);
//        job_1.setMapOutputValueClass(TextArrayWritable.class);
//
//        job_1.setNumReduceTasks(30);
//
//        addInputPath(job_1, new Path(args[0]));
//        setOutputPath(job_1, tmpPath);
//
//        job_1.setJarByClass(MergeFlights.class);
//        job_1.waitForCompletion(true);

        Job job_2 = getInstance(conf, "Merge flights x -> y -> z");

        job_2.setMapperClass(MergeMapper.class);
        job_2.setReducerClass(MergeReducer.class);

        job_2.setMapOutputKeyClass(Text.class);
        job_2.setMapOutputValueClass(NullWritable.class);

        job_2.setOutputKeyClass(Text.class);
        job_2.setOutputValueClass(Text.class);
        job_2.setOutputFormatClass(NullOutputFormat.class);

        addInputPath(job_2, new Path(args[0]));

        job_2.setJarByClass(MergeFlightsXYZ.class);

        job_2.setNumReduceTasks(3);

        exit(job_2.waitForCompletion(true) ? 0 : 1);
    }
}
