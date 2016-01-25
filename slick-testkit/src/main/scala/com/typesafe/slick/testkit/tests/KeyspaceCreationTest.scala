package com.typesafe.slick.testkit.tests

import com.typesafe.slick.testkit.util.{CassandraTestDB, AsyncTest}

class KeyspaceTest extends AsyncTest[CassandraTestDB] {
  import tdb.profile.api._

  def testKeyspaceCreation = {
    val command = "CREATE KEYSPACE IF NOT EXISTS slicktest WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"
    val action = SimpleDBIO(_.connection.execute(command))
    for {
      _ <- action
    } yield ()
  }

  def testKeyspaceDrop = {
    val command = "DROP KEYSPACE slicktest;"
    val action = SimpleDBIO(_.connection.execute(command))
    for {
      _ <- action
    } yield ()
  }
}
