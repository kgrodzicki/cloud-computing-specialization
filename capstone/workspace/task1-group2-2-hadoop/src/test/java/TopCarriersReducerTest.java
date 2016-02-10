import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.junit.Test;


/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 30/01/16.
 */
public class TopCarriersReducerTest {

    @Test
    public void testCassandraInsert() {
        TopDestReducer reducer = new TopDestReducer();
        TreeBidiMap<Double, String> topCarriers = new TreeBidiMap<>();
        topCarriers.put(1., "AW");
        topCarriers.put(2., "LA");
        topCarriers.put(2., "LA");
        topCarriers.put(2., "LA");
        topCarriers.put(2., "LA");
        topCarriers.put(2., "LA");
        topCarriers.put(2., "LA");
        reducer.persist("NYC", topCarriers);
    }
}
