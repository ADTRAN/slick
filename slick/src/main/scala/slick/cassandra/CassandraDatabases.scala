package slick.cassandra

import slick.util.AsyncExecutor
import scala.concurrent.ExecutionContext
import org.reactivestreams.Subscriber

trait CassandraDatabases {self: CassandraBackend =>

  abstract class CassandraDatabase(val executor: AsyncExecutor) {
    /** Create the default DatabaseActionContext for this backend. */
    protected[this] def createDatabaseActionContext[T](_useSameThread: Boolean): self.Context =
      new CassandraActionContext { val useSameThread = _useSameThread }

    /** Return the default ExecutionContext for this Database which should be used for running
      * SynchronousDatabaseActions for asynchronous execution. */
    protected[this] def synchronousExecutionContext: ExecutionContext = executor.executionContext
  }

  /** A database instance to which connections can be created, which uses
    * zookeeper to keep track of the cassandra nodes. */
  class ZookeeperDatabase(override val executor: AsyncExecutor,
    val zookeeperLocation: String,
    val zNode: String,
    val timeout: Int,
    val retryTime: Int) extends CassandraDatabase(executor) {

    /** Create a new session. The session needs to be closed explicitly by calling its close() method. */
    def createSession(): Session = {
      new ZookeeperSessionDef(zookeeperLocation, zNode, timeout, retryTime)
    }

    def close: Unit = {
    }
  }

  /** A database instance to which connections can be created, which takes a
    * list of cassandra nodes directly. */
  class DirectDatabase(override val executor: AsyncExecutor,
    val nodes: List[String],
    val timeout: Int,
    val retryTime: Int) extends CassandraDatabase(executor) {

    /** Create a new session. The session needs to be closed explicitly by calling its close() method. */
    def createSession(): Session = {
      new DirectSessionDef(nodes, timeout, retryTime)
    }

    def close: Unit = {
    }
  }
}
