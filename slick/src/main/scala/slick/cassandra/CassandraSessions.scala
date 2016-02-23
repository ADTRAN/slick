package slick.cassandra
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.state.ConnectionStateListener
import scala.concurrent.{Promise, ExecutionContext, Future, Await}
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions._
import slick.SlickException
import scala.concurrent.duration._
import com.datastax.driver.core.Cluster
import scala.language.postfixOps
import scala.util.{Try, Failure}

abstract class CassandraSessionDef {

  val connection: Future[Session]
  def close(): Unit
  def force(): Unit
  def run(cql: String): Unit = {
    Await.result(connection, 1 minute) execute cql
  }

  def buildCluster(nodes: Iterator[String], builder: Cluster.Builder): Cluster = {
    def addNodes(builder: Cluster.Builder, nodes: Iterator[String]): Cluster.Builder = {
      if (nodes.hasNext) {
        val Array(ip, port) = nodes.next().split(':')
        addNodes(builder.addContactPoint(ip).withPort(port.toInt), nodes)
      } else {
        builder
      }
    }

    addNodes(builder, nodes).build
  }
}

class DirectSession(val nodes: List[String],
  val keyspace: Option[String],
  val builder: Cluster.Builder = Cluster.builder) extends CassandraSessionDef {

  val connectionPromise = Promise[Session]
  val connection = connectionPromise.future
  val cluster = buildCluster(nodes.iterator, builder)
  val session = keyspace map {keyspace => cluster.connect(keyspace)} getOrElse cluster.connect
  connectionPromise trySuccess session

  def close(): Unit = {
  }

  def force(): Unit = {
  }
}

import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.RetryPolicy

class ZookeeperSession(val zookeeperLocation: String,
  val zNode: String,
  val keyspace: Option[String],
  val timeout: Int,
  val retryTime: Int,
  val builder: Cluster.Builder = Cluster.builder,
  val newClient: (String, RetryPolicy) => CuratorFramework = CuratorFrameworkFactory.newClient _,
  val newNodeCache: (CuratorFramework, String) => NodeCache = (x, y) => new NodeCache(x, y)) extends CassandraSessionDef
                         with NodeCacheListener
                         with ConnectionStateListener {

  import org.apache.curator.framework.api.CuratorEvent
  import org.apache.curator.framework.state.ConnectionState
  import org.apache.curator.retry.RetryUntilElapsed
  import com.datastax.driver.core.Cluster
  import java.util.concurrent.Executors
  import java.util.concurrent.TimeUnit.MILLISECONDS
  import java.net.InetAddress

  def validateInetAddress(ipAndPort: String, message: String): Unit = {
    val Array(ip, port) = ipAndPort.split(':')
    try {
      InetAddress.getByName(ip)
      port.toInt
    } catch {
      case e: Exception => throw new SlickException(s"Incorrect format for $message address.  Must be address:port")
    }
  }

  val connectionPromise = Promise[Session]
  val connection = connectionPromise.future

  validateInetAddress(zookeeperLocation, "zookeeper")

  val zk = newClient(zookeeperLocation, new RetryUntilElapsed(timeout, retryTime))
  zk.getConnectionStateListenable.addListener(this)

  val nodeCache = newNodeCache(zk, zNode)
  nodeCache.getListenable.addListener(this)
  zk.start()

  val scheduler = Executors.newScheduledThreadPool(1)
  val connectionTimeout = new Runnable {
    override def run(): Unit = {
      val failed = connectionPromise tryFailure (new SlickException(s"Initial connection to zookeeper or cassandra timed out, or zNode not found: $zNode"))
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
    if (connectionPromise.isCompleted) {
      throw new SlickException("Changing cassandra's zNode while a connection is in use is not currently supported.")
    }

    val data = new String(nodeCache.getCurrentData.getData)

    val session = Try {
      data.lines foreach {node => validateInetAddress(node, "cassandra")}
      val cluster = buildCluster(data.lines, builder)
      keyspace map {keyspace => cluster.connect(keyspace)} getOrElse cluster.connect
    }

    session map (connectionPromise success _)

    val errorMessage = session match {
      case Failure(e: NoHostAvailableException) => Some("Host addresses or keyspace incorrect or unreachable connecting to cassandra:  ")
      case _                                    => None
    }

    session recover {
      case e => connectionPromise tryFailure (new SlickException(errorMessage getOrElse "" + e.getMessage))
    }
  }

  def close(): Unit = {
    if (connection.isCompleted)
      Await.result(connection, Duration.Zero).close()
    zk.close()
  }

  def force(): Unit = {
  }
}
