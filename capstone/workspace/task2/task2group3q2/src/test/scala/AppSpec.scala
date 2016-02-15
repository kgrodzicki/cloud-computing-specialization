import App.Model.Flight
import org.scalatest._


class AppSpec extends FunSuite with Matchers {

  test("An empty Set should have size 0") {
    assert(App.parseDate("2000-01-01") != null)
  }

  test("parsing empty srting throws runtime exception") {
    intercept[RuntimeException] {
      App.parseDate("")
    }
  }

  test("isCorrect") {
    val firstLeg = Flight("NYC", "HOU", App.parseDate("2008-01-01"), 1111, "4", 0.0)
    val secondLeg = Flight("HOU", "LIT", App.parseDate("2008-01-03"), 1201, "3", -10.0)
    assert(App.isCorrect(firstLeg, secondLeg))
  }

}
