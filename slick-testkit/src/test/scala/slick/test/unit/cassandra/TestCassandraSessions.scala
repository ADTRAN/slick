package test.scala.slick.test.unit.cassandra
import org.scalatest._
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import time.SpanSugar._
import org.scalamock.scalatest.MockFactory
import slick.cassandra._
import com.datastax.driver.core.{Cluster, Session, AbstractSession, Configuration}
import com.datastax.driver.core.exceptions._
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.curator.framework.state.ConnectionState
import slick.SlickException
import scala.concurrent.Await

class CassandraSessionTests extends WordSpec with MockFactory with ScalaFutures {

  class MockableCluster extends Cluster(null, null, null)

  class MockSession extends Session {
    var closeCalled = false

    def close: Unit = {
      closeCalled = true
    }

    def closeAsync(): com.datastax.driver.core.CloseFuture = ???
    def execute(x$1: com.datastax.driver.core.Statement): com.datastax.driver.core.ResultSet = ???
    def execute(x$1: String,x$2: Object*): com.datastax.driver.core.ResultSet = ???
    def execute(x$1: String): com.datastax.driver.core.ResultSet = ???
    def executeAsync(x$1: com.datastax.driver.core.Statement): com.datastax.driver.core.ResultSetFuture = ???
    def executeAsync(x$1: String,x$2: Object*): com.datastax.driver.core.ResultSetFuture = ???
    def executeAsync(x$1: String): com.datastax.driver.core.ResultSetFuture = ???
    def getCluster(): com.datastax.driver.core.Cluster = ???
    def getLoggedKeyspace(): String = ???
    def getState(): com.datastax.driver.core.Session.State = ???
    def init(): com.datastax.driver.core.Session = ???
    def isClosed(): Boolean = ???
    def newSimpleStatement(x$1: String,x$2: Object*): com.datastax.driver.core.SimpleStatement = ???
    def newSimpleStatement(x$1: String): com.datastax.driver.core.SimpleStatement = ???
    def prepare(x$1: com.datastax.driver.core.RegularStatement): com.datastax.driver.core.PreparedStatement = ???
    def prepare(x$1: String): com.datastax.driver.core.PreparedStatement = ???
    def prepareAsync(x$1: com.datastax.driver.core.RegularStatement): com.google.common.util.concurrent.ListenableFuture[com.datastax.driver.core.PreparedStatement] = ???
    def prepareAsync(x$1: String): com.google.common.util.concurrent.ListenableFuture[com.datastax.driver.core.PreparedStatement] = ???
  }

  class MockClusterBuilder extends Cluster.Builder {
    var hasContactPoints = false
    var shouldThrowNoHostAvailable = false
    override def addContactPoint(ip: String): Cluster.Builder = {
      if (ip startsWith "500") {
        throw new IllegalArgumentException
      } else if (ip startsWith "400") {
        throw new SlickException("")
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

      val map = new java.util.HashMap[java.net.InetSocketAddress, Throwable]()
      if (shouldThrowNoHostAvailable) {
        (cluster.connect _) when() throws new NoHostAvailableException(map)
        (cluster.connect(_: String)) when("slicktest") throws new NoHostAvailableException(map)
      } else {
        (cluster.connect _) when() returns new MockSession
        (cluster.connect(_: String)) when("slicktest") returns new MockSession
        (cluster.connect(_: String)) when("invalid") throws new NoHostAvailableException(map)
      }

      cluster
    }
  }

  import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
  import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
  import org.apache.curator.RetryPolicy

  val mockFramework = stub[CuratorFramework]

  def mockNewClient(zookeeperLocation: String, retryPolicy: RetryPolicy): CuratorFramework = {
    import org.apache.curator.framework.listen.ListenerContainer
    import org.apache.curator.framework.state.ConnectionStateListener

    val connectionStateListenable = new ListenerContainer[ConnectionStateListener]
    (mockFramework.getConnectionStateListenable _) when() returns(connectionStateListenable)

    mockFramework
  }

  class MockNodeCache extends NodeCache(null, "/") {
    import org.apache.curator.framework.listen.{ListenerContainer, Listenable}
    import org.apache.curator.framework.recipes.cache.ChildData
    import org.apache.zookeeper.data.Stat

    override def getListenable: ListenerContainer[NodeCacheListener] = new ListenerContainer[NodeCacheListener]
    override def start: Unit = {}
    override def close: Unit = {}

    override def getCurrentData: ChildData = new ChildData("/cassandra", new Stat, "127.0.0.1:9042".getBytes)
  }

  class InvalidMockNodeCache extends MockNodeCache {
    import org.apache.curator.framework.recipes.cache.ChildData
    import org.apache.zookeeper.data.Stat

    override def getCurrentData: ChildData = new ChildData("/cassandra", new Stat, "400.0.0.1:9042".getBytes)
  }

  def mockNodeCache = (zk: CuratorFramework, zNode: String) => new MockNodeCache

  def invalidMockNodeCache = (zk: CuratorFramework, zNode: String) => new InvalidMockNodeCache

  "A DirectSession" when {
    "given a valid node" should {
      "return a successful future" in {
        val builder  = new MockClusterBuilder

        val session = new DirectSession(List("127.0.0.1:9042"), Some("slicktest"), builder)
        assert(session.connection.isReadyWithin(100 millis))
      }
    }

    "given an invalid node" should {
      "throw an IllegalArgumentException" in {
        val builder = new MockClusterBuilder

        intercept[IllegalArgumentException] {
          val session = new DirectSession(List("500.0.0.2:9042"), Some("slicktest"), builder)
        }
      }
    }

    "given an unreachable node" should {
      "throw an NoHostAvailableException" in {
        val builder = new MockClusterBuilder

        intercept[NoHostAvailableException] {
          val session = new DirectSession(List("127.0.0.2:9042"), Some("slicktest"), builder)
        }
      }
    }

    "given an empty list" should {
      "throw an NoHostAvailableException" in {
        val builder = new MockClusterBuilder

        intercept[NoHostAvailableException] {
          val session = new DirectSession(List(), Some("slicktest"), builder)
        }
      }
    }

    "given a valid node but no keyspace" should {
      "return a successful future" in {
        val builder  = new MockClusterBuilder

        val session = new DirectSession(List("127.0.0.1:9042"), None, builder)
        assert(session.connection.isReadyWithin(100 millis))
      }
    }

    "given a valid node but an invalid keyspace" should {
      "throw an NoHostAvailableException" in {
        val builder = new MockClusterBuilder

        intercept[NoHostAvailableException] {
          val session = new DirectSession(List("127.0.0.1:9042"), Some("invalid"), builder)
        }
      }
    }
  }

  "a ZookeeperSession" when {
    "given a valid zookeeper address with a valid cassandra zNode" should {
      "return a successful future" in {
        val builder  = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", Some("slicktest"), 10000, 100, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        assert(session.connection.isReadyWithin(100 millis))
      }
    }

    "given an invalid zookeeper address" should {
      "throw a SlickException" in {
        val builder  = new MockClusterBuilder

        val thrown = intercept[SlickException] {
          val session = new ZookeeperSession("500.0.0.1:2181", "/cassandra", Some("slicktest"), 10000, 100, builder, mockNewClient, mockNodeCache)
        }
        thrown.getMessage should startWith ("Incorrect format for zookeeper address.")
      }
    }

    "given an unreachable zookeeper address" should {
      "fail the connection future with SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("192.168.0.1:2181", "/cassandra", Some("slicktest"), 10, 1, builder, mockNewClient, mockNodeCache)
        whenReady(session.connection.failed) {e =>
          e shouldBe a [SlickException]
          e.getMessage should startWith ("Initial connection to zookeeper or cassandra timed out")
        }
      }
    }

    "given an invalid zNode path" should {
      "fail the connection future with SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassondry", Some("slicktest"), 10, 1, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        whenReady(session.connection.failed) {e =>
          e shouldBe a [SlickException]
          e.getMessage should startWith ("Initial connection to zookeeper or cassandra timed out")
        }
      }
    }

    "given a valid zNode path with an invalid cassandra address" should {
      "fail the connection future with SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", Some("slicktest"), 10000, 100, builder, mockNewClient, invalidMockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        whenReady(session.connection.failed) {e =>
          e shouldBe a [SlickException]
          e.getMessage should startWith ("Incorrect format for cassandra address.")
        }
      }
    }

    "given a valid zNode that is later changed after making a good connection" should {
      "throw a SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", Some("slicktest"), 10000, 100, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        val thrown = intercept[SlickException] {
          session.nodeChanged()
        }
        thrown.getMessage should startWith ("Changing cassandra's zNode while a connection is in use is not currently supported.")
      }
    }

    "given a valid configuration but a slow link to zookeeper" should {
      "fail the connection future with a SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", Some("slicktest"), 10, 1, builder, mockNewClient, mockNodeCache)
        whenReady(session.connection.failed) {e =>
          e shouldBe a [SlickException]
          e.getMessage should startWith ("Initial connection to zookeeper or cassandra timed out")
        }
      }
    }

    "given a valid configuration, a good link to zookeeper, but a slow link to cassandra" should {
      "fail the connection future with a SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", Some("slicktest"), 10, 1, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        whenReady(session.connection.failed) {e =>
          e shouldBe a [SlickException]
          e.getMessage should startWith ("Initial connection to zookeeper or cassandra timed out")
        }
      }
    }

    "closing the slick session" should {
      "close a completed cassandra connection" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", Some("slicktest"), 10000, 100, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        session.close()
        assert(Await.result(session.connection, 0 millis).asInstanceOf[MockSession].closeCalled)
      }

      "close the zookeeper connection" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", Some("slicktest"), 10000, 100, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        session.close()
        var closed = false
        (mockFramework.close _) when() returns({closed = true})
        assert(closed)
      }
    }

    "given a valid zookeeper address with a valid cassandra zNode but no keyspace" should {
      "return a successful future" in {
        val builder  = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", None, 10000, 100, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        assert(session.connection.isReadyWithin(100 millis))
      }
    }

    "given a valid zookeeper address with a valid cassandra zNode but an invalid keyspace" should {
      "fail the connection future with a SlickException" in {
        val builder  = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", Some("invalid"), 10000, 100, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        whenReady(session.connection.failed) {e =>
          e shouldBe a [SlickException]
          e.getMessage should startWith ("Host addresses or keyspace incorrect or unreachable connecting to cassandra")
        }
      }
    }
  }
}
