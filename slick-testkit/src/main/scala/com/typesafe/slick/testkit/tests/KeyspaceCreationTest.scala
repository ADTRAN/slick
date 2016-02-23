package com.typesafe.slick.testkit.tests

import com.typesafe.slick.testkit.util.{CassandraTestDB, AsyncTest}

class KeyspaceTest extends AsyncTest[CassandraTestDB] {
  import tdb.profile.api._

  def testKeyspaceCreation = {
    val dropIfCommand = "DROP KEYSPACE IF EXISTS slicktest;"
    val dropCommand   = "DROP KEYSPACE slicktest;"
    val createCommand = "CREATE KEYSPACE IF NOT EXISTS slicktest WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"
    val dropIfAction  = SimpleDBIO(_.connection.execute(dropIfCommand))
    val dropAction  = SimpleDBIO(_.connection.execute(dropCommand))
    val createAction  = SimpleDBIO(_.connection.execute(createCommand))
    val actions = DBIO.seq(dropIfAction, createAction, dropAction, createAction) // recreate at end for other tests
    for {
      _ <- actions
    } yield ()
  }
}
