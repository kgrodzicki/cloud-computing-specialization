import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.policies.DefaultRetryPolicy.INSTANCE;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class FlightInfoReducer extends Reducer<Text, TextArrayWritable, Text, NullWritable> {

    public static Logger log = Logger.getLogger(FlightInfoReducer.class);

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

    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        String origin = origin(key.toString());
        String dest = dest(key.toString());
        String dateStr = departureDate(key.toString());
        LocalDate depDate = date(dateStr);

        String[] flightData = getBestFlightWithMinArrDelay(values);
        if (flightData[0] != null) {
            persist(origin, dest, depDate, toInt(flightData[0]), toInt(flightData[1]), toInt(flightData[2]));
            context.write(new Text(mergeToKey(origin, dest, dateStr, flightData[0], flightData[1], flightData[2])),
                    NullWritable.get());
        }
    }

    private String mergeToKey(String origin, String dest, String dateStr, String flightNum, String depTime, String arrDelay) {
        return origin + TokenizerMapper.SEPARATOR +
                dest + TokenizerMapper.SEPARATOR +
                dateStr + TokenizerMapper.SEPARATOR +
                flightNum + TokenizerMapper.SEPARATOR +
                depTime + TokenizerMapper.SEPARATOR +
                arrDelay;
    }

    private String[] getBestFlightWithMinArrDelay(Iterable<TextArrayWritable> values) {
        String[] bestFlight = new String[3];
        Double minArrDelay = Double.MAX_VALUE;
        for (TextArrayWritable each : values) {
            Text[] texts = (Text[]) each.toArray();
            String flightNum = texts[0].toString();
            String depTime = texts[1].toString();
            String s = texts[2].toString();
            if (StringUtils.isEmpty(s)) {
                continue;
            }
            Double arrDelay = Double.valueOf(s);

            if (minArrDelay > arrDelay) {
                minArrDelay = arrDelay;
                bestFlight[0] = flightNum;
                bestFlight[1] = depTime;
                bestFlight[2] = String.valueOf(arrDelay);
            }
        }
        return bestFlight;
    }

    private Integer toInt(String arrDelay) {
        if (StringUtils.isNotBlank(arrDelay)) {
            return Double.valueOf(arrDelay).intValue();
        }
        return null;
    }

    LocalDate date(String dateStr) {
        String[] split = dateStr.split("-");
        return LocalDate.fromYearMonthDay(Integer.valueOf(split[0]), Integer.valueOf(split[1]), Integer.valueOf(split[2]));
    }

    @SuppressWarnings("MethodWithTooManyParameters")
    void persist(String origin, String dest, LocalDate flightDate, Integer flightNum, Integer depTime, Integer arrDelay) {
        String tableName;
        if (depTime <= 1200) {
            tableName = "capstone.flightinfo_dest";
        } else {
            tableName = "capstone.flightinfo_origin";
        }

        String cqlQuery = "INSERT INTO " + tableName + " (origin, dest, flightDate, FlightNum, depTime, arrDelay) VALUES (?,"
                + "?,?,?,?,?)";
        PreparedStatement preparedStatement = connect.prepare(cqlQuery);
        BoundStatement boundStatement = preparedStatement.bind(origin, dest, flightDate, flightNum, depTime, arrDelay);
        connect.executeAsync(boundStatement);
    }

    String departureDate(String s) {
        return s.split(TokenizerMapper.SEPARATOR)[2];
    }

    String dest(String s) {
        return s.split(TokenizerMapper.SEPARATOR)[1];
    }

    String origin(String s) {
        return s.split(TokenizerMapper.SEPARATOR)[0];
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        connect.close();
    }
}
