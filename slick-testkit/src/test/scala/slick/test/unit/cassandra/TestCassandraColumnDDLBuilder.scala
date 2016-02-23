package test.scala.slick.test.unit.cassandra
import org.scalatest._
import Matchers._
import org.scalamock.scalatest.MockFactory
import slick.cassandra._
import CassandraProfile._
import slick.SlickException
import slick.ast.{FieldSymbol, ColumnOption}

class CassandraColumnDDLBuilderTests extends WordSpec with MockFactory {
  "A Cassandra ColumnDDLBuilder" when {
    "given an INT column that is not a primary key" should {
      "have a toString of 'columnName int'" in {
        val symbol = FieldSymbol("columnName")(Seq(), CassandraIntColumnType)
        val builder = new ColumnDDLBuilder(symbol)
        builder.toString shouldBe ("columnName int")
      }
    }

    "given an INT column that is a primary key" should {
      "have a toString of 'columnName int PRIMARY KEY'" in {
        val symbol = FieldSymbol("columnName")(Seq(ColumnOption.PrimaryKey), CassandraIntColumnType)
        val builder = new ColumnDDLBuilder(symbol)
        builder.toString shouldBe ("columnName int PRIMARY KEY")
      }
    }

    "given a column with an unsupported type" should {
      "throw a SlickException" in {
        val symbol = FieldSymbol("columnName")(Seq(), CassandraStringColumnType)
        val thrown = intercept[SlickException] {
          new ColumnDDLBuilder(symbol)
        }
        thrown.getMessage should startWith ("CassandraProfile has no CassandraType for type ")
      }
    }
  }
}
