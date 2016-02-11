import com.datastax.driver.core.LocalDate;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MergeReducerTest {


    @Test
    public void testGetFlightInfo() {
        MergeReducer reducer = new MergeReducer();
        LocalDate date = LocalDate.fromYearMonthDay(2008, 1, 3);
        String dest = "POR";
//        String origin = "NYC";
//        assertTrue(reducer.getFlightInfo(origin, null, date).size() > 0);
//        assertTrue(reducer.getFlightInfo(null, dest, date).size() > 0);
    }

    @Test
    public void testDate() {
        MergeReducer reducer = new MergeReducer();

        LocalDate localDate = reducer.localDate(LocalDate.fromDaysSinceEpoch(1).toString());
        assertNotNull(localDate);
    }

    @Test
    public void checkRoute(){
        MergeReducer reducer = new MergeReducer();
        assertTrue(reducer.valid("NYC", "WRO", "WAW"));
        assertFalse(reducer.valid("WAW", "WRO", "WAW"));
        assertFalse(reducer.valid("WRO", "WRO", "WAW"));
    }
}
