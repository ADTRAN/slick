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

/** Backend for the basic database and session handling features.
  * Concrete backends like `JdbcBackend` extend this type and provide concrete
  * types for `Database`, `DatabaseFactory` and `Session`. */
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
    * The supported config keys are backend-specific. This method is used by `DatabaseConfig`.
    * @param path The path in the configuration file for the database configuration, or an empty
    *             string for the top level of the `Config` object.
    * @param config The `Config` object to read from.
    */
  def createDatabase(config: Config, path: String): Database = Database.forConfig(path, config)

  /** A database instance to which connections can be created. */
  class DatabaseDef(val executor: AsyncExecutor, val zookeeperLocation: String) extends super.DatabaseDef { this: Database =>
    /** Create a new session. The session needs to be closed explicitly by calling its close() method. */
    def createSession(): Session = {
      new SessionDef(zookeeperLocation)
    }

    /** Free all resources allocated by Slick for this Database, blocking the current thread until
      * everything has been shut down.
      *
      * Backend implementations which are based on a naturally blocking shutdown procedure can
      * simply implement this method and get `shutdown` as an asynchronous wrapper for free. If
      * the underlying shutdown procedure is asynchronous, you should implement `shutdown` instead
      * and wrap it with `Await.result` in this method. */
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
    def forConfig(path: String, config: Config): Database = {
      val usedConfig = if (path.isEmpty) config else config.getConfig(path)
      val zookeeperLocation = config.getString("zookeeper")
      new DatabaseDef(AsyncExecutor.default(), zookeeperLocation)
    }
  }

  import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}

  class SessionDef(val zookeeperLocation: String) extends super.SessionDef  with NodeCacheListener {
    import org.apache.curator.framework.CuratorFrameworkFactory
    import org.apache.curator.retry.RetryForever
    import com.datastax.driver.core.Cluster

    val zk = CuratorFrameworkFactory.newClient(zookeeperLocation, new RetryForever(5000))
    zk.start()

    val nodeCache = new NodeCache(zk, "/cassandra")
    nodeCache.getListenable().addListener(this)
    nodeCache.start()
    val connectionPromise = Promise[CassandraSession]
    val connection = connectionPromise.future

    def nodeChanged: Unit = {
      def addNodes(builder: Cluster.Builder, nodes: Iterator[String]): Cluster.Builder = {
        if (nodes.hasNext) {
          val Array(ip, port) = nodes.next().split(':')
          addNodes(builder.addContactPoint(ip), nodes)
        } else {
          builder
        }
      }

      val data = new String(nodeCache.getCurrentData.getData)
      val nodes = data.lines
      val cluster = addNodes(Cluster.builder, nodes).build
      connectionPromise success cluster.connect
    }

    def close(): Unit = {
      if (connection.isCompleted)
        Await.result(connection, Duration.Zero).close()
      nodeCache.close()
      zk.close()
    }

    def force(): Unit = {
    }
  }

  /** The context object passed to database actions by the execution engine. */
  trait CassandraActionContext extends BasicActionContext {
    def connection: CassandraSession = Await.result(session.connection, Duration(1, MINUTES))
  }

  /** A special DatabaseActionContext for streaming execution. */
  class CassandraStreamingActionContext(subscriber: Subscriber[_], useSameThread: Boolean, database: Database) extends BasicStreamingActionContext(subscriber, useSameThread, database) with CassandraActionContext
}

object CassandraBackend extends CassandraBackend
