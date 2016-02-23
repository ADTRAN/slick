package test.scala.slick.test.unit.cassandra
import org.scalatest._
import Matchers._
import org.scalamock.scalatest.MockFactory
import slick.cassandra._
import CassandraProfile.api._
import slick.SlickException
import slick.ast.{FieldSymbol, ColumnOption}
import slick.lifted.BaseTag

class CassandraTableDDLTests extends WordSpec with MockFactory {
  "A Cassandra TableDDL" when {
    "given a single INT column that is a primary key" should {
      "have a createPhase1 of 'Iterable('CREATE TABLE TableName (ColumnName int PRIMARY KEY');'" in {
        class TestTable(tag: slick.lifted.Tag) extends Table[(Int)](tag, "TableName") {
          def id = column[Int]("ColumnName", O.PrimaryKey)

          def * = (id)
        }

        val table = new TestTable(null)
        val ddl = new CassandraTableDDL(table)
        ddl.createPhase1 should contain ("CREATE TABLE TableName (ColumnName int PRIMARY KEY);")
      }
    }
  }

  //Note:  Negative tests and other column and primary key configurations will
  //       be handled in the next user story.
}
