import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.ConsistencyLevel.ONE;
import static com.datastax.driver.core.policies.DefaultRetryPolicy.INSTANCE;
import static com.google.common.collect.Lists.newArrayList;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class TopCarriersReducer extends Reducer<Text, TextArrayWritable, Text, Text> {

    public static Logger log = Logger.getLogger(TopCarriersReducer.class);

    private static final Cluster cluster = builder()
            .withCredentials("cassandra", "cassandra")
            .addContactPoints("52.91.166.84", "54.173.255.179")
            .withRetryPolicy(INSTANCE)
            .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
            .withQueryOptions(new QueryOptions().setConsistencyLevel(ONE))
            .build();

    private Session connect = cluster.connect("capstone");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        connect = cluster.connect("capstone");
    }

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        String origin = key.toString();
        TreeBidiMap<Double, String> topCarriers = new TreeBidiMap<>();
        for (TextArrayWritable each : values) {
            Text[] texts = (Text[]) each.toArray();
            String carrier = texts[0].toString();
            Double performance = Double.valueOf(texts[1].toString());
            topCarriers.put(performance, carrier);
            if (topCarriers.size() > 10) {
                topCarriers.remove(topCarriers.firstKey());
            }
        }

        persist(origin, topCarriers);
    }

    void persist(String origin, TreeBidiMap<Double, String> topCarriers) {
        String cqlQuery = "INSERT INTO capstone.airport (code, top_carriers) VALUES (?,?)";
        PreparedStatement preparedStatement = connect.prepare(cqlQuery);
        Object[] values = topCarriers.values().toArray();
        CollectionUtils.reverseArray(values);
        BoundStatement boundStatement = preparedStatement.bind(origin, newArrayList(values));
        connect.executeAsync(boundStatement);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        connect.close();
    }
}
