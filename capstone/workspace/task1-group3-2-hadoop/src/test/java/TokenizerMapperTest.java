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

        List<ImmutablePair<String, List<String>>> pairs = tokenizerMapper.parse("LAX,SFO,2008-01-03,593,2321,203.00,0.00\n"
                + "LAX,SFO,2008-01-03,618,,,1.00\n");

        assertTrue(pairs.get(0).left.equals("LAX#SFO#2008-01-03#AFTER_12"));
    }
}
