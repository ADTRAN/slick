package test.scala.slick.test.unit.cassandra
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.concurrent.AsyncAssertions.Waiter
import time.SpanSugar._
import org.scalamock.scalatest.MockFactory
import slick.cassandra._
import com.datastax.driver.core.{Cluster, Session, AbstractSession, Configuration}
import com.datastax.driver.core.exceptions._
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.curator.framework.state.ConnectionState
import slick.SlickException
import scala.concurrent.Await

class CassandraSessionTests extends WordSpec with MockFactory {

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

      if (shouldThrowNoHostAvailable) {
        val map = new java.util.HashMap[java.net.InetSocketAddress, Throwable]()
        (cluster.connect _) when() throws new NoHostAvailableException(map)
      } else {
        (cluster.connect _) when() returns new MockSession
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

        val session = new DirectSession(List("127.0.0.1:9042"), builder)
        assert(session.connection.isReadyWithin(100 millis))
      }

    }

    "given an invalid node" should {
      "throw an IllegalArgumentException" in {
        val builder = new MockClusterBuilder

        intercept[IllegalArgumentException] {
          val session = new DirectSession(List("500.0.0.2:9042"), builder)
        }
      }
    }

    "given an unreachable node" should {
      "throw an NoHostAvailableException" in {
        val builder = new MockClusterBuilder

        intercept[NoHostAvailableException] {
          val session = new DirectSession(List("127.0.0.2:9042"), builder)
        }
      }
    }

    "given an empty list" should {
      "throw an NoHostAvailableException" in {
        val builder = new MockClusterBuilder

        intercept[NoHostAvailableException] {
          val session = new DirectSession(List(), builder)
        }
      }
    }
  }

  "a ZookeeperSession" when {
    "given a valid zookeeper address with a valid cassandra zNode" should {
      "return a successful future" in {
        val builder  = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        assert(session.connection.isReadyWithin(100 millis))
      }
    }

    "given an invalid zookeeper address" should {
      "throw a SlickException" in {
        val builder  = new MockClusterBuilder

        intercept[SlickException] {
          val session = new ZookeeperSession("500.0.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, mockNodeCache)
        }
      }
    }

    "given an unreachable zookeeper address" should {
      "fail the connection future with SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("192.168.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, mockNodeCache)
        val w = new Waiter
        session.connection onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[SlickException] {
          w.await
        }
      }
    }

    "given an invalid zNode path" should {
      "fail the connection future with SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassondry", 10, 1, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        val w = new Waiter
        session.connection onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[SlickException] {
          w.await
        }
      }
    }

    "given a valid zNode path with an invalid cassandra address" should {
      "fail the connection future with SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, invalidMockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        val w = new Waiter
        session.connection onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[SlickException] {
          w.await
        }
      }
    }

    "given a valid zNode that is later changed after making a good connection" should {
      "throw a SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        intercept[SlickException] {
          session.nodeChanged()
        }
      }
    }

    "given a valid configuration but a slow link to zookeeper" should {
      "fail the connection future with a SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, mockNodeCache)
        val w = new Waiter
        session.connection onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[SlickException] {
          w.await
        }
      }
    }

    "given a valid configuration, a good link to zookeeper, but a slow link to cassandra" should {
      "fail the connection future with a SlickException" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        val w = new Waiter
        session.connection onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[SlickException] {
          w.await
        }
      }
    }

    "closing the slick session" should {
      "close a completed cassandra connection" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        session.close()
        assert(Await.result(session.connection, 0 millis).asInstanceOf[MockSession].closeCalled)
      }

      "close the zookeeper connection" in {
        val builder = new MockClusterBuilder

        val session = new ZookeeperSession("127.0.0.1:2181", "/cassandra", 10, 1, builder, mockNewClient, mockNodeCache)
        session.stateChanged(mockFramework, ConnectionState.CONNECTED)
        session.nodeChanged()
        session.close()
        var closed = false
        (mockFramework.close _) when() returns({closed = true})
        assert(closed)
      }
    }
  }
}
