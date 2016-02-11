import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.datastax.driver.core.Cluster.builder;
import static com.datastax.driver.core.LocalDate.fromYearMonthDay;
import static com.datastax.driver.core.policies.DefaultRetryPolicy.INSTANCE;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static java.util.Calendar.DAY_OF_MONTH;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 25/01/16.
 */
public class MergeReducer extends Reducer<Text, NullWritable, Text, Text> {

    public static final String INSERT_INTO_CAPSTONE_FLIGHTCONNECTIONS_X_Y_Z_X_DEP_DATE_Y_DEP_DATE_XYFLIGHTNR_YZFLIGHTNR_VALUES
            = "INSERT INTO capstone.flightconnections (x, y, z, xDepDate, yDepDate, xyflightnr, yzflightnr) VALUES (?,?,?,?,?,"
            + "?,?)";

    public static Logger log = Logger.getLogger(MergeReducer.class);

    private static final Cluster cluster = builder()
            .withCredentials("cassandra", "cassandra")
            .addContactPoints("52.91.166.84", "54.173.255.179")
            .withRetryPolicy(INSTANCE)
            .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
            .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
            .build();

    private Session connect;

    PreparedStatement preparedStatement;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        connect = cluster.connect("capstone");
        preparedStatement = connect.prepare(
                INSERT_INTO_CAPSTONE_FLIGHTCONNECTIONS_X_Y_Z_X_DEP_DATE_Y_DEP_DATE_XYFLIGHTNR_YZFLIGHTNR_VALUES);
    }

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException,
            InterruptedException {
        String[] texts = MergeMapper.parse(key.toString());

        final String origin = texts[0];
        final String dest = texts[1];
        String dateStr = texts[2];
        final LocalDate depDate = localDate(dateStr);
        final Integer flightNum = Integer.valueOf(texts[3]);
        String depTime = texts[4];
//        String arrDelay = texts[5];

        if (Integer.valueOf(depTime) < 1200) {
            LocalDate nextDepDate = depDate.add(DAY_OF_MONTH, 2);
            final ResultSetFuture rows = getFlightInfo(dest, null, nextDepDate);
            addCallback(rows, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(@Nullable ResultSet result) {
                    saveOriginDestZ(origin, dest, depDate, flightNum, result);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("Timeout for request");
//                    throw new RuntimeException("retry");
                }
            }, sameThreadExecutor());
        } else {
            LocalDate twoDaysBack = depDate.add(DAY_OF_MONTH, -2);
            ResultSetFuture rows = getFlightInfo(null, origin, twoDaysBack);

            addCallback(rows, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(@Nullable ResultSet result) {
                    saveXOriginDest(origin, dest, depDate, flightNum, result);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("Timeout for request");
//                    throw new RuntimeException("retry");
                }
            }, sameThreadExecutor());
        }
    }

    private void saveXOriginDest(String origin, String dest, LocalDate depDate, Integer flightNum, ResultSet rows) {
        for (Row eachFlight : rows) {
            String x = eachFlight.getString("dest");
            LocalDate xDepDate = eachFlight.getDate("flightdate");
            int xyFlightNr = eachFlight.getInt("flightnum");
            if (valid(x, origin, dest)) {
                persist(x, origin, dest, xDepDate, depDate, xyFlightNr, flightNum);
            }
        }
    }

    private void saveOriginDestZ(String origin, String dest, LocalDate depDate, Integer flightNum, ResultSet rows) {
        for (Row eachFlight : rows) {
            String z = eachFlight.getString("dest");
            LocalDate yDepDate = eachFlight.getDate("flightdate");
            int yzFlightNr = eachFlight.getInt("flightnum");
            if (valid(origin, dest, z)) {
                persist(origin, dest, z, depDate, yDepDate, flightNum, yzFlightNr);
            }
        }
    }

    boolean valid(String x, String y, String z) {
        return newHashSet(x.toLowerCase(), y.toLowerCase(), z.toLowerCase()).size() == 3;
    }

    LocalDate localDate(String dateStr) {
        String[] split = dateStr.split("-");
        return fromYearMonthDay(Integer.valueOf(split[0]), Integer.valueOf(split[1]), Integer.valueOf(split[2]));
    }

    ResultSetFuture getFlightInfo(String origin, String dest, LocalDate flightDate) {
        Statement statement;
        if (origin != null) {
            statement = QueryBuilder
                    .select()
                    .all()
                    .from("capstone", "flightinfo_origin")
                    .where(eq("origin", origin))
                    .and(eq("flightdate", flightDate));
        } else {
            statement = QueryBuilder
                    .select()
                    .all()
                    .from("capstone", "flightinfo_dest")
                    .allowFiltering()
                    .where(eq("dest", dest))
                    .and(eq("flightdate", flightDate));
        }

        return connect.executeAsync(statement);
    }

    void persist(String x, String y, String z, LocalDate xDepDate, LocalDate yDepDate, Integer xyFlightNr, Integer yzFlightNr) {
        connect.executeAsync(preparedStatement.bind(x, y, z, xDepDate, yDepDate, xyFlightNr, yzFlightNr));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        connect.close();
    }
}
