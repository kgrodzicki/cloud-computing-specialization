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

        List<ImmutablePair<String, Integer>> pairs = tokenizerMapper.parse("HOU,-1.00,LIT\n"
                + "HOU,,SAT\n"
                + "HOU,1.00,AAA");

        assertTrue(pairs.size() == 3);
        assertTrue(pairs.get(0).left.equals("HOU-LIT"));
        assertTrue(pairs.get(0).right == 1);
        assertTrue(pairs.get(1).left.equals("HOU-SAT"));
        assertTrue(pairs.get(1).right == 0);
        assertTrue(pairs.get(2).left.equals("HOU-AAA"));
        assertTrue(pairs.get(2).right == 0);
    }
}
