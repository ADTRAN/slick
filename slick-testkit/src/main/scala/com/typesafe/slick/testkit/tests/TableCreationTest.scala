package com.typesafe.slick.testkit.tests

import com.typesafe.slick.testkit.util.{CassandraTestDB, AsyncTest}

class TableCreationTest extends AsyncTest[CassandraTestDB] {
  import tdb.profile.api._

  def testTableCreation = {
    class TestTable(tag: Tag) extends Table[Int](tag, "TEST") {
      def id = column[Int]("ID")
      def * = id
    }
    val testTable = TableQuery(new TestTable(_))
    for {
      _ <- testTable.schema.create
    } yield ()
  }
}
