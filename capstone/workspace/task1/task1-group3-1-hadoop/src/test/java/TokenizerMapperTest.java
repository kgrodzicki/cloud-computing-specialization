import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TokenizerMapperTest {

    @Test
    public void givenString_whenCodes_whenTokenized() {
        TokenizerMapper tokenizerMapper = new TokenizerMapper();

        List<String> codes = tokenizerMapper.codes("OSL,WRO\nWRO,OSL\nWRO,NYC");

        assertTrue(codes.contains("OSL"));
        assertTrue(codes.contains("WRO"));
        assertTrue(codes.contains("NYC"));
        assertTrue(codes.size() == 6);
    }
}
