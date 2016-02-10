import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.policies.DefaultRetryPolicy.INSTANCE;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ROUND_FLOOR;
import static java.math.BigInteger.*;
import static java.math.BigInteger.ONE;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class MeanReducer extends Reducer<Text, IntWritable, Text, Text> {

    public static Logger log = Logger.getLogger(MeanReducer.class);

    public static final String INSERT_INTO_CAPSTONE_MEAN_ARR_DELAY_ORIGIN_DEST_MEAN_VALUES = "INSERT INTO capstone.meanArrDelay"
            + " (origin, dest, mean) VALUES (?,?,?)";

    private static final Cluster cluster = builder()
            .withCredentials("cassandra", "cassandra")
            .addContactPoints("52.91.166.84", "54.173.255.179")
            .withRetryPolicy(INSTANCE)
            .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
            .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
            .build();

    private Session connect = cluster.connect("capstone");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        connect = cluster.connect("capstone");
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        BigInteger count = ZERO;
        BigInteger sum = ZERO;

        for (IntWritable each : values) {
            if (each.get() > 0) {
                sum = sum.add(valueOf(each.get()));
                count = count.add(ONE);
            }
        }

        persist(key.toString(), sum, count);
    }

    void persist(String key, BigInteger sum, BigInteger count) {
        BigDecimal mean = mean(sum, count);

        PreparedStatement preparedStatement = connect.prepare(INSERT_INTO_CAPSTONE_MEAN_ARR_DELAY_ORIGIN_DEST_MEAN_VALUES);
        BoundStatement boundStatement = preparedStatement.bind(origin(key), dest(key), mean.intValue());
        connect.executeAsync(boundStatement);
    }

    private BigDecimal mean(BigInteger sum, BigInteger count) {
        BigDecimal mean = BigDecimal.ZERO;
        if (!sum.equals(ZERO) && !count.equals(ZERO)) {
            mean = new BigDecimal(sum).divide(new BigDecimal(count), ROUND_FLOOR);
        }
        return mean;
    }

    private String dest(String s) {
        return s.split("-")[1];
    }

    private String origin(String s) {
        return s.split("-")[0];
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        connect.close();
    }
}
