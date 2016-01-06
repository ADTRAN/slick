package slick.cassandra
import slick.relational.RelationalBackend

import scala.language.existentials

import java.io.Closeable
import java.util.concurrent.atomic.{AtomicReferenceArray, AtomicBoolean, AtomicLong}

import com.typesafe.config.Config

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Promise, ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory
import org.reactivestreams._

import slick.SlickException
import slick.dbio._
import slick.util._

trait CassandraBackend extends RelationalBackend {

  type This = CassandraBackend
  type Database = DatabaseDef
  type DatabaseFactory = DatabaseFactoryDef
  type Context = CassandraActionContext
  type StreamingContext = CassandraStreamingActionContext

  // This is a session in the Slick sense, not in the Cassandra/Datastax sense.
  type Session = SessionDef
  // For convenience working inside this file.
  type CassandraSession = com.datastax.driver.core.Session

  val Database: DatabaseFactory = new DatabaseFactoryDef {}
  val backend: CassandraBackend = this

  /** Create a Database instance through [[https://github.com/typesafehub/config Typesafe Config]].
    * This method is used by `DatabaseConfig`.
    * Supported config keys:
    * nodes:              Array of "IPv4 address:port" containing the location
    *                     of the cassandra nodes.  Must specify either 'nodes'
    *                     or 'zookeeper', but not both.  Default is to use
    *                     zookeeper.
    * zookeeper:          IPv4 address pointing to zookeeper instance with
    *                     cassandra config.  Defaults to '127.0.0.1'
    * zNode:              zNode containing location of cassandra nodes in
    *                     zookeeper. defaults to '/cassandra'
    * timeout:            milliseconds to wait for connection before aborting.
    *                     Defaults to 60000.
    * retryTime:          milliseconds to wait between connection retries.
    *                     Defaults to 5000.
    * @param path The path in the configuration file for the database configuration, or an empty
    *             string for the top level of the `Config` object.
    * @param config The `Config` object to read from.
    */
  def createDatabase(config: Config, path: String): Database = Database.forConfig(path, config)

  abstract class DatabaseDef extends super.DatabaseDef {
  }

  /** A database instance to which connections can be created, which uses
    * zookeeper to keep track of the cassandra nodes. */
  class ZookeeperDatabaseDef(val executor: AsyncExecutor,
    val zookeeperLocation: String,
    val zNode: String,
    val timeout: Int,
    val retryTime: Int) extends DatabaseDef { this: Database =>

    /** Create a new session. The session needs to be closed explicitly by calling its close() method. */
    def createSession(): Session = {
      new ZookeeperSessionDef(zookeeperLocation, zNode, timeout, retryTime)
    }

    def close: Unit = {
    }

    /** Create the default DatabaseActionContext for this backend. */
    protected[this] def createDatabaseActionContext[T](_useSameThread: Boolean): Context =
      new CassandraActionContext { val useSameThread = _useSameThread }

    /** Create the default StreamingDatabaseActionContext for this backend. */
    protected[this] def createStreamingDatabaseActionContext[T](s: Subscriber[_ >: T], useSameThread: Boolean): StreamingContext =
      new CassandraStreamingActionContext(s, useSameThread, this)

    /** Return the default ExecutionContext for this Database which should be used for running
      * SynchronousDatabaseActions for asynchronous execution. */
    protected[this] def synchronousExecutionContext: ExecutionContext = executor.executionContext
  }

  /** A database instance to which connections can be created, which takes a
    * list of cassandra nodes directly. */
  class DirectDatabaseDef(val executor: AsyncExecutor,
    val nodes: List[String],
    val timeout: Int,
    val retryTime: Int) extends DatabaseDef { this: Database =>

    /** Create a new session. The session needs to be closed explicitly by calling its close() method. */
    def createSession(): Session = {
      new DirectSessionDef(nodes, timeout, retryTime)
    }

    def close: Unit = {
    }

    /** Create the default DatabaseActionContext for this backend. */
    protected[this] def createDatabaseActionContext[T](_useSameThread: Boolean): Context =
      new CassandraActionContext { val useSameThread = _useSameThread }

    /** Create the default StreamingDatabaseActionContext for this backend. */
    protected[this] def createStreamingDatabaseActionContext[T](s: Subscriber[_ >: T], useSameThread: Boolean): StreamingContext =
      new CassandraStreamingActionContext(s, useSameThread, this)

    /** Return the default ExecutionContext for this Database which should be used for running
      * SynchronousDatabaseActions for asynchronous execution. */
    protected[this] def synchronousExecutionContext: ExecutionContext = executor.executionContext
  }

  trait DatabaseFactoryDef {
    import com.typesafe.config.ConfigFactory
    import scala.collection.convert.wrapAll._

    val defaults: Config = ConfigFactory.parseMap(Map(
      "zookeeper"          -> "127.0.0.1",
      "zNode"              -> "/cassandra",
      "timeout"   -> 60000,
      "retryTime" -> 5000))

    def forConfig(path: String, config: Config): Database = {
      val usedConfig = if (path.isEmpty) config else config.getConfig(path)
      val fallbackConfig = usedConfig withFallback defaults
      val timeout = fallbackConfig.getInt("timeout")
      val retryTime = fallbackConfig.getInt("retryTime")

      if (fallbackConfig.hasPath("nodes")) {
        val nodes = fallbackConfig.getStringList("nodes").toList
        new DirectDatabaseDef(AsyncExecutor.default(), nodes, timeout, retryTime)
      } else {
        val zookeeperLocation = fallbackConfig.getString("zookeeper")
        val zNode = fallbackConfig.getString("zNode")
        new ZookeeperDatabaseDef(AsyncExecutor.default(), zookeeperLocation, zNode, timeout, retryTime)
      }
    }
  }

  import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
  import org.apache.curator.framework.state.ConnectionStateListener

  abstract class SessionDef extends super.SessionDef {
    import com.datastax.driver.core.Cluster

    val connection: Future[CassandraSession]

    def buildCluster(nodes: Iterator[String]): Cluster = {
      def addNodes(builder: Cluster.Builder, nodes: Iterator[String]): Cluster.Builder = {
        if (nodes.hasNext) {
          val Array(ip, port) = nodes.next().split(':')
          addNodes(builder.addContactPoint(ip), nodes)
        } else {
          builder
        }
      }

      addNodes(Cluster.builder, nodes).build
    }
  }

  class DirectSessionDef(val nodes: List[String],
                         val timeout: Int,
                         val retryTime: Int) extends SessionDef {

    val connectionPromise = Promise[CassandraSession]
    val connection = connectionPromise.future
    val cluster = buildCluster(nodes.iterator)
    connectionPromise trySuccess cluster.connect

    def close(): Unit = {
    }

    def force(): Unit = {
    }
  }

  class ZookeeperSessionDef(val zookeeperLocation: String,
                            val zNode: String,
                            val timeout: Int,
                            val retryTime: Int) extends SessionDef
                                                   with NodeCacheListener
                                                   with ConnectionStateListener {

    import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
    import org.apache.curator.framework.api.CuratorEvent
    import org.apache.curator.framework.state.ConnectionState
    import org.apache.curator.retry.RetryUntilElapsed
    import com.datastax.driver.core.Cluster
    import java.util.concurrent.Executors
    import java.util.concurrent.TimeUnit.MILLISECONDS

    val connectionPromise = Promise[CassandraSession]
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

  /** The context object passed to database actions by the execution engine. */
  trait CassandraActionContext extends BasicActionContext {
    def connection: CassandraSession = Await.result(session.connection, Duration.Inf) // Give Session control over failure timeout period
  }

  /** A special DatabaseActionContext for streaming execution. */
  class CassandraStreamingActionContext(subscriber: Subscriber[_], useSameThread: Boolean, database: Database) extends BasicStreamingActionContext(subscriber, useSameThread, database) with CassandraActionContext
}

object CassandraBackend extends CassandraBackend
