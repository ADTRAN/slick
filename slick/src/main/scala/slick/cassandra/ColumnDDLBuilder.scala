package slick.cassandra
import slick.ast._
import slick.sql.SqlProfile

class ColumnDDLBuilder(column: FieldSymbol) {
  val primaryKey = column.options contains ColumnOption.PrimaryKey
  val CassandraType(cassandraType) = column.tpe

  override def toString: String = {
    val primaryKeyString = if (primaryKey) " PRIMARY KEY" else ""
    column.name + " " + cassandraType.cqlTypeName + primaryKeyString
  }
}
