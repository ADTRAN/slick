package slick.cassandra

class CassandraTableDDL(val table: CassandraProfile#Table[_]) {
  val columns: Iterable[String] = table.create_*.map(column => new ColumnDDLBuilder(column)).map(_.toString)
  val tableNode = table.tableNode

  def createPhase1: Iterable[String] = {
    val name = tableNode.tableName
    val cql = "CREATE TABLE " + name + " (" + columns.mkString(", ") + ");"
    Iterable(cql)
  }
  def createPhase2: Iterable[String] = Iterable()
  def dropPhase1: Iterable[String] = ???
  def dropPhase2: Iterable[String] = ???
}
