import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 26/01/16.
 */
public class PerformanceReducerTest {

    PerformanceReducer reducer = new PerformanceReducer();

    @Test
    public void givenkeysAndValues_whenUpdateCache_thenUpdatedAndSorted() {
        reducer.setMaxElements(3);

        reducer.updateCache(40, "4444");
        reducer.updateCache(10, "1111");
        reducer.updateCache(20, "2222");
        reducer.updateCache(30, "3333");
        reducer.updateCache(100, "1111");

        TreeBidiMap<Double, String> cache = reducer.getCache();
        assertTrue(cache.size() == 3);
        assertTrue(cache.containsKey(100.));
        assertTrue(cache.containsValue("1111"));
        assertTrue(cache.containsKey(40.));
        assertTrue(cache.containsValue("4444"));
        assertTrue(cache.containsKey(30.));
        assertTrue(cache.containsValue("3333"));
    }

    @Test
    public void giveOnTimeAndNotOntim_whenPerformance_thenPercentage() {
        assertTrue(reducer.performance(40, 60) == 40.);
        assertTrue(reducer.performance(99, 1) == 99.);
    }
}
