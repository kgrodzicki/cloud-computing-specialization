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

        List<ImmutablePair<String, Integer>> pairs = tokenizerMapper.parse("HOU,LIT,16.00\n"
                + "HOU,NYC,10.00\n");

        assertTrue(pairs.size() == 2);
        assertTrue(pairs.get(0).left.equals("HOU-LIT"));
        assertTrue(pairs.get(0).right == 16);
        assertTrue(pairs.get(1).left.equals("HOU-NYC"));
        assertTrue(pairs.get(1).right == 10);
    }
}
