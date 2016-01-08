package slick.cassandra
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.state.ConnectionStateListener
import scala.concurrent.{Promise, ExecutionContext, Future, Await}
import com.datastax.driver.core.Session
import slick.SlickException
import scala.concurrent.duration._

abstract class CassandraSessionDef {
  import com.datastax.driver.core.Cluster

  val connection: Future[Session]
  def close(): Unit
  def force(): Unit

  def buildCluster(nodes: Iterator[String]): Cluster = {
    def addNodes(builder: Cluster.Builder, nodes: Iterator[String]): Cluster.Builder = {
      if (nodes.hasNext) {
        val Array(ip, port) = nodes.next().split(':')
        addNodes(builder.addContactPoint(ip).withPort(port.toInt), nodes)
      } else {
        builder
      }
    }

    addNodes(Cluster.builder, nodes).build
  }
}

class DirectSession(val nodes: List[String],
  val timeout: Int,
  val retryTime: Int) extends CassandraSessionDef {

  val connectionPromise = Promise[Session]
  val connection = connectionPromise.future
  val cluster = buildCluster(nodes.iterator)
  connectionPromise trySuccess cluster.connect

  def close(): Unit = {
  }

  def force(): Unit = {
  }
}

class ZookeeperSession(val zookeeperLocation: String,
  val zNode: String,
  val timeout: Int,
  val retryTime: Int) extends CassandraSessionDef
                         with NodeCacheListener
                         with ConnectionStateListener {

  import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
  import org.apache.curator.framework.api.CuratorEvent
  import org.apache.curator.framework.state.ConnectionState
  import org.apache.curator.retry.RetryUntilElapsed
  import com.datastax.driver.core.Cluster
  import java.util.concurrent.Executors
  import java.util.concurrent.TimeUnit.MILLISECONDS

  val connectionPromise = Promise[Session]
  val connection = connectionPromise.future

  val zk = CuratorFrameworkFactory.newClient(zookeeperLocation, new RetryUntilElapsed(timeout, retryTime))
  zk.getConnectionStateListenable.addListener(this)

  val nodeCache = new NodeCache(zk, zNode)
  nodeCache.getListenable.addListener(this)
  zk.start()

  val scheduler = Executors.newScheduledThreadPool(1)
  val connectionTimeout = new Runnable {
    override def run(): Unit = {
      val failed = connectionPromise tryFailure (new SlickException("Could not make initial connection to zookeeper to retrieve cassandra configuration"))
      if (failed) {
        zk.close()
      }
    }
  }
  scheduler.schedule(connectionTimeout, timeout, MILLISECONDS)

  def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = newState match {
    case ConnectionState.CONNECTED |
    ConnectionState.RECONNECTED => nodeCache.start()
    case ConnectionState.LOST |
    ConnectionState.SUSPENDED   => nodeCache.close()
    case ConnectionState.READ_ONLY   => // Don't care
  }

  def nodeChanged: Unit = {
    val data = new String(nodeCache.getCurrentData.getData)
    val nodes = data.lines
    val cluster = buildCluster(nodes)
    connectionPromise trySuccess cluster.connect
  }

  def close(): Unit = {
    if (connection.isCompleted)
      Await.result(connection, Duration.Zero).close()
    zk.close()
  }

  def force(): Unit = {
  }
}
