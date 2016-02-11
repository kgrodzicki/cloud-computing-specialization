import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SumReducerTest {

    @Test
    public void givenkeysAndValues_whenUpdateCache_thenUpdatedAndSorted() {
        SumReducer reducer = new SumReducer();
        reducer.setMaxElements(3);

        reducer.updateCache(10, "test");
        reducer.updateCache(2, "test2");
        reducer.updateCache(3, "test3");
        reducer.updateCache(4, "test4");
        reducer.updateCache(5, "test2");

        TreeBidiMap<Integer, String> cache = reducer.getCache();
        assertTrue(cache.size() == 3);
        assertTrue(cache.containsKey(5));
        assertTrue(cache.containsValue("test2"));
        assertTrue(cache.containsKey(10));
        assertTrue(cache.containsValue("test"));
        assertTrue(cache.containsKey(4));
        assertTrue(cache.containsValue("test4"));
    }
}
