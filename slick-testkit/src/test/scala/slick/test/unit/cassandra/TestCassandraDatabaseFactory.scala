package test.scala.slick.test.unit.cassandra
import org.scalatest._
import org.scalamock.scalatest.MockFactory
import slick.cassandra._
import slick.SlickException
import com.typesafe.config.{Config, ConfigFactory}

class CassandraDatabaseFactoryTests extends WordSpec
                                    with MockFactory
                                    with CassandraBackend
                                    with CassandraDatabaseFactory {

  "A CassandraDatabaseFactory" when {
    "given a config with a zookeeper address" should {
      "return a zookeeper database" in {

        val config = ConfigFactory.parseString("""
          zookeeper = "127.0.0.1:2181"
          zNode     = "/cassandra"
          timeout   = 0
          retryTime = 0
          """)

        val factory  = new DatabaseFactoryDef {}
        val database = factory.forConfig("", config)

        assert(database.isInstanceOf[ZookeeperDatabaseDef])
      }
    }

    "given a config with direct cassandra nodes" should {
      "return a direct database" in {

        val config = ConfigFactory.parseString("""
          nodes     = ["127.0.0.1:9042"]
          timeout   = 0
          retryTime = 0
          """)

        val factory  = new DatabaseFactoryDef {}
        val database = factory.forConfig("", config)

        assert(database.isInstanceOf[DirectDatabaseDef])
      }
    }

    "given a config with both direct cassandra nodes and a zookeeper address" should {
      "throw a SlickException" in {

        val config = ConfigFactory.parseString("""
          zookeeper = "127.0.0.1:2181"
          nodes     = ["127.0.0.1:9042"]
          timeout   = 0
          retryTime = 0
          """)

        val factory  = new DatabaseFactoryDef {}

        intercept[SlickException] {
          val database = factory.forConfig("", config)
        }
      }
    }
  }
}
