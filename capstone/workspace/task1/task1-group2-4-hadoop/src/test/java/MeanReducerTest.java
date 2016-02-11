import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 26/01/16.
 */
public class MeanReducerTest {

    MeanReducer reducer = new MeanReducer();

    @Test
    public void testMean() {
        BigInteger sum = new BigInteger("100");
        BigInteger count = new BigInteger("10");
        reducer.persist("NYC-POR", sum, count);
    }
}
