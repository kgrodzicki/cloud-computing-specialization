import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 26/01/16.
 */
public class FlightInfoReducerTest {

    FlightInfoReducer reducer = new FlightInfoReducer();

    @Test
    @Ignore
    public void testMean() {
        BigInteger sum = new BigInteger("100");
        BigInteger count = new BigInteger("10");
        // 2008-01-03,588,1343,16.00,0.00
        reducer.persist("NYC", "POR", reducer.date("2008-01-03"), 58, 1200, 16);
    }

    @Test
    public void testParsing() {
        String key = "LAX#SFO#2008-01-03#AFTER_12";
        assertEquals("LAX", reducer.origin(key));
        assertEquals("SFO", reducer.dest(key));
        assertEquals("2008-01-03", reducer.departureDate(key));
    }
}
