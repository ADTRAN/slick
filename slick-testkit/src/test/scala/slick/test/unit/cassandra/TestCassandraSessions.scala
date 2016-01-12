package test.scala.slick.test.unit.cassandra
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.concurrent.AsyncAssertions.Waiter
import time.SpanSugar._
import org.scalamock.scalatest.MockFactory
import slick.cassandra._
import com.datastax.driver.core.{Cluster, Session, Configuration}
import com.datastax.driver.core.exceptions._
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

class CassandraDirectSessionTests extends WordSpec with MockFactory {

  class MockableCluster extends Cluster(null, null, null)

  class MockClusterBuilder extends Cluster.Builder {
    var hasContactPoints = false
    var shouldThrowNoHostAvailable = false
    override def addContactPoint(ip: String): Cluster.Builder = {
      if (ip == "500.0.0.2") {
        throw new IllegalArgumentException
      } else if (ip == "127.0.0.2") {
        shouldThrowNoHostAvailable = true
        this
      } else {
        hasContactPoints = true
        this
      }
    }

    override def withPort(port: Int): Cluster.Builder = {
      this
    }

    override def build: Cluster = {
      val cluster = stub[MockableCluster]
      if (!hasContactPoints) {
        shouldThrowNoHostAvailable = true
      }

      if (shouldThrowNoHostAvailable) {
        val map = new java.util.HashMap[java.net.InetSocketAddress, Throwable]()
        (cluster.connect _) when() throws new NoHostAvailableException(map)
      }

      cluster
    }
  }

  "A DirectSession" when {
    "given a valid node" should {
      "return a successful future" in {
        val builder  = new MockClusterBuilder

        val session = new DirectSession(List("127.0.0.1:9042"), builder)
        assert(session.connection.isReadyWithin(100 millis))
      }

    }

    "given an invalid node" should {
      "fail the connection future with IllegalArgumentException" in {
        val builder = new MockClusterBuilder

        val session = new DirectSession(List("500.0.0.2:9042"), builder)
        val w = new Waiter
        session.connection onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[IllegalArgumentException] {
          w.await
        }
      }
    }

    "given an unreachable node" should {
      "fail the connection future with NoHostAvailableException" in {
        val builder = new MockClusterBuilder

        val session = new DirectSession(List("127.0.0.2:9042"), builder)
        val w = new Waiter
        session.connection onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[NoHostAvailableException] {
          w.await
        }
      }
    }

    "given an empty list" should {
      "fail the connection future with NoHostAvailableException" in {
        val builder = new MockClusterBuilder

        val session = new DirectSession(List(), builder)
        val w = new Waiter
        session.connection onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[NoHostAvailableException] {
          w.await
        }
      }
    }
  }
}
