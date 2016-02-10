import org.junit.Test;

public class SumReducerTest {

//    @Test
//    public void givenkeysAndValues_whenUpdateCache_thenUpdatedAndSorted() {
//        SumReducer reducer = new SumReducer();
//
//        reducer.updateCache(10, "test");
//        reducer.updateCache(2, "test2");
//        reducer.updateCache(3, "test3");
//        reducer.updateCache(4, "test4");
//        reducer.updateCache(5, "test2");
//
//        TreeBidiMap<Integer, String> cache = reducer.getCache();
//        assertTrue(cache.size() == 3);
//        assertTrue(cache.containsKey(5));
//        assertTrue(cache.containsValue("test2"));
//        assertTrue(cache.containsKey(10));
//        assertTrue(cache.containsValue("test"));
//        assertTrue(cache.containsKey(4));
//        assertTrue(cache.containsValue("test4"));
//    }

    @Test
    public void testPersist() {
        SumReducer reducer = new SumReducer();
        reducer.persist(81923, "NYC");
    }
}
