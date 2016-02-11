import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 30/01/16.
 */
public class TokenizerMapperTest {

    @Test
    public void givenString_whenCodes_whenTokenized() {
        TokenizerMapper tokenizerMapper = new TokenizerMapper();

        List<ImmutablePair<String, Integer>> pairs = tokenizerMapper.parse("LAS,WN,23.00,0.00\n"
                + "LAS,WN,0.00,1.00\n"
                + "WRO,AA,0.00,0.00\n"
                + "WRO,BB,-10.00,0.00\n"
                + "WRO,CC,,0.00\n");

        assertTrue(pairs.size() == 4);
        assertTrue(pairs.get(0).left.equals("LAS-WN"));
        assertTrue(pairs.get(0).right == 0);
        assertTrue(pairs.get(1).left.equals("WRO-AA"));
        assertTrue(pairs.get(1).right == 1);
        assertTrue(pairs.get(2).left.equals("WRO-BB"));
        assertTrue(pairs.get(2).right == 1);
        assertTrue(pairs.get(3).left.equals("WRO-CC"));
        assertTrue(pairs.get(3).right == 1);
    }
}
