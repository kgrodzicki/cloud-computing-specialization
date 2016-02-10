import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 26/01/16.
 */
public class PerformanceReducerTest {

    PerformanceReducer reducer = new PerformanceReducer();

    @Test
    public void giveOnTimeAndNotOntim_whenPerformance_thenPercentage() {
        assertTrue(reducer.performance(40, 60) == 40.);
        assertTrue(reducer.performance(99, 1) == 99.);
    }
}
