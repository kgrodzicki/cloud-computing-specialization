import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.policies.DefaultRetryPolicy.INSTANCE;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public static final String INSERT_INTO_CAPSTONE_AIRPORT_POPULARITY_CODE_POPULARITY_VALUES = "INSERT INTO capstone"
            + ".airportPopularity (code, popularity) VALUES (?,?)";

    private TreeBidiMap<Integer, String> cache = new TreeBidiMap<>();

    private static final Cluster cluster = builder()
            .withCredentials("cassandra", "cassandra")
            .addContactPoints("52.91.166.84", "54.173.255.179")
            .withRetryPolicy(INSTANCE)
            .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
            .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
            .build();

    private Session connect;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        connect = cluster.connect("capstone");
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        persist(sum(values), key.toString());
    }

    void updateCache(Integer sum, String code) {
//        if (cache.containsValue(code)) {
//            cache.removeValue(code);
//        }
//        cache.put(sum, code);
//        if (cache.size() > maxElements) {
//            cache.remove(cache.firstKey());
//        }
    }

    void persist(Integer sum, String code) {
        PreparedStatement preparedStatement = connect.prepare(INSERT_INTO_CAPSTONE_AIRPORT_POPULARITY_CODE_POPULARITY_VALUES);
        BoundStatement boundStatement = preparedStatement.bind(code, sum);
        connect.executeAsync(boundStatement);
    }

    private int sum(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        return sum;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        connect.close();
    }


    public TreeBidiMap<Integer, String> getCache() {
        return cache;
    }
}
