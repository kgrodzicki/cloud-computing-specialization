/**
 * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 01/02/16.
 */
public enum DepartureType {
    BEFRORE_12, AFTER_12;

    public static DepartureType build(int time) {
        if (time < 1200) {
            return BEFRORE_12;
        } else {
            return AFTER_12;
        }
    }
}
