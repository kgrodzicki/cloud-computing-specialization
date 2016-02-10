import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class TokenizerMapperTest {

    @Test
    public void givenString_whenCodes_whenTokenized() {
        TokenizerMapper tokenizerMapper = new TokenizerMapper();

        List<ImmutablePair<String, Integer>> pairs = tokenizerMapper.codes("19393,1451,0.00\n"
                + "19394,,1.00\n"
                + "19395,-2136,0.00\n"
                + "19396,0.00,0.00");

        assertTrue(pairs.get(0).left.equals("19393"));
        assertTrue(pairs.get(0).right == 0);
        assertTrue(pairs.get(1).left.equals("19394"));
        assertTrue(pairs.get(1).right == 0);
        assertTrue(pairs.get(2).left.equals("19395"));
        assertTrue(pairs.get(2).right == 1);
        assertTrue(pairs.get(3).left.equals("19396"));
        assertTrue(pairs.get(3).right == 1);
    }
}
