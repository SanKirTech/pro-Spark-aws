import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OperationsTest extends FunSuite {

  test("Testing Sum Function") {
    assert(Operations.sum(-5,-3) === -8)
    assert(Operations.sum(0,-3) === -3)
    assert(Operations.sum(-5,0) === -5)
    assert(Operations.sum(-5,3) === -2)
    assert(Operations.sum(5,-3) === 2)
    assert(Operations.sum(5,3) === 8)
  }
}
