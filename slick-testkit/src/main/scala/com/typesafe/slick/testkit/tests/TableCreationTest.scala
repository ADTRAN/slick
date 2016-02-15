package com.typesafe.slick.testkit.tests

import com.typesafe.slick.testkit.util.{CassandraTestDB, AsyncTest}

class TableCreationTest extends AsyncTest[CassandraTestDB] {
  import tdb.profile.api._

  def testTableCreation = {
    class TestTable(tag: Tag) extends Table[Int](tag, "TEST") {
      def id = column[Int]("ID", O.PrimaryKey)
      def * = id
    }
    val testTable = TableQuery(new TestTable(_))
    val check = "SELECT * FROM TEST;"
    val cleanup = "DROP TABLE TEST;"
    val checkAction = SimpleDBIO(_.connection.execute(check))
    val cleanupAction = SimpleDBIO(_.connection.execute(cleanup))

    for {
      _ <- testTable.schema.create
      _ <- checkAction
      _ <- cleanupAction
    } yield ()
  }
}
