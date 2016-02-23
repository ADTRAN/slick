package slick.cassandra
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.collection.convert.wrapAll._
import slick.util.AsyncExecutor
import slick.SlickException
import scala.util.Try

trait CassandraDatabaseFactory {self: CassandraBackend =>
  trait DatabaseFactoryDef {

    def forConfig(path: String, config: Config): self.DatabaseDef = {
      val usedConfig = if (path.isEmpty) config else config.getConfig(path)
      val timeout = usedConfig.getInt("timeout")
      val retryTime = usedConfig.getInt("retryTime")
      val keyspace: Option[String] = Try(usedConfig.getString("keyspace")).toOption

      if (usedConfig.hasPath("nodes") && usedConfig.hasPath("zookeeper"))
        throw new SlickException("Config cannot contain both zookeeper node and direct cassandra entries.")

      if (usedConfig.hasPath("nodes")) {
        val nodes = usedConfig.getStringList("nodes").toList
        new self.DirectDatabaseDef(AsyncExecutor.default(), nodes, keyspace, timeout, retryTime)
      } else {
        val zookeeperLocation = usedConfig.getString("zookeeper")
        val zNode = usedConfig.getString("zNode")
        new self.ZookeeperDatabaseDef(AsyncExecutor.default(), zookeeperLocation, zNode, keyspace, timeout, retryTime)
      }
    }
  }
}
