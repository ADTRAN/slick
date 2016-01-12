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

trait CassandraBackend extends RelationalBackend
                          with CassandraDatabaseFactory
                          with CassandraDatabases {

  type This = CassandraBackend
  type Database = DatabaseDef
  type DatabaseFactory = DatabaseFactoryDef
  type Context = CassandraActionContext
  type StreamingContext = CassandraStreamingActionContext
  val backend: CassandraBackend = this

  // This is a session in the Slick sense, not in the Cassandra/Datastax sense.
  type Session = SessionDef

  val Database: DatabaseFactory = new DatabaseFactoryDef {}

  /** Create a Database instance through [[https://github.com/typesafehub/config Typesafe Config]].
    * This method is used by `DatabaseConfig`.
    * Supported config keys:
    * nodes:              Array of "IPv4 address:port" containing the location
    *                     of the cassandra nodes.  Must specify either 'nodes'
    *                     or 'zookeeper', but not both.  Default is to use
    *                     zookeeper.
    * zookeeper:          IPv4 address:port pointing to zookeeper instance with
    *                     cassandra config.  Defaults to '127.0.0.1:2181'
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

  trait DatabaseDef extends CassandraDatabase with super.DatabaseDef {
    /** Create the default StreamingDatabaseActionContext for this backend. */
    protected[this] def createStreamingDatabaseActionContext[T](s: Subscriber[_ >: T], useSameThread: Boolean): StreamingContext =
      new CassandraStreamingActionContext(s, useSameThread, this)
  }

  class ZookeeperDatabaseDef(override val executor: AsyncExecutor,
                             override val zookeeperLocation: String,
                             override val zNode: String,
                             override val timeout: Int,
                             override val retryTime: Int)
    extends ZookeeperDatabase(executor, zookeeperLocation, zNode, timeout, retryTime)
    with DatabaseDef

  class DirectDatabaseDef(override val executor: AsyncExecutor,
                          override val nodes: List[String],
                          override val timeout: Int,
                          override val retryTime: Int)
    extends DirectDatabase(executor, nodes, timeout, retryTime)
    with DatabaseDef

  trait SessionDef extends CassandraSessionDef with super.SessionDef

  class ZookeeperSessionDef (override val zookeeperLocation: String,
                             override val zNode: String,
                             override val timeout: Int,
                             override val retryTime: Int)
    extends ZookeeperSession(zookeeperLocation, zNode, timeout, retryTime)
    with SessionDef

  class DirectSessionDef (override val nodes: List[String])
    extends    DirectSession(nodes)
    with SessionDef

  /** The context object passed to database actions by the execution engine. */
  trait CassandraActionContext extends BasicActionContext {
    def connection: com.datastax.driver.core.Session = Await.result(session.connection, Duration.Inf) // Give Session control over failure timeout period
  }

  /** A special DatabaseActionContext for streaming execution. */
  class CassandraStreamingActionContext(subscriber: Subscriber[_], useSameThread: Boolean, database: Database) extends BasicStreamingActionContext(subscriber, useSameThread, database) with CassandraActionContext
}

object CassandraBackend extends CassandraBackend
