package slick.cassandra
import slick.basic._
import slick.sql._

import scala.language.{higherKinds, implicitConversions, existentials}

import slick.SlickException
import slick.ast._
import slick.compiler.QueryCompiler
import slick.dbio._
import slick.lifted._
import slick.util.{GlobalConfig, DumpInfo}

import com.typesafe.config.{ConfigFactory, Config}

trait CassandraProfile extends SqlProfile with CassandraActionComponent
  with CassandraTypesComponent {

  type Backend = CassandraBackend
  val backend: Backend = CassandraBackend

  type ColumnType[T] = CassandraType[T]
  type BaseColumnType[T] = CassandraType[T] with BaseTypedType[T]

  override protected def computeCapabilities: Set[Capability] = Set.empty

  val api: API = new API {}

  lazy val queryCompiler  = compiler
  lazy val updateCompiler = compiler
  lazy val deleteCompiler = compiler
  lazy val insertCompiler = compiler

  type CompiledInsert = CassandraCompiledInsert
  def compileInsert(n: Node): CompiledInsert = new CassandraCompiledInsert()
  class CassandraCompiledInsert {}

  def runSynchronousQuery[R](tree: Node,param: Any)(implicit session: Backend#Session): R = ???

  def buildSequenceSchemaDescription(seq: Sequence[_]): SchemaDescription = new SequenceDDL

  def buildTableSchemaDescription(table: Table[_]): SchemaDescription = new TableDDL(table)

  lazy val MappedColumnType: MappedColumnTypeFactory = MappedCassandraType

  case class SimpleCassandraAction[+R](f: CassandraBackend#Context => R) extends SynchronousDatabaseAction[R, NoStream, CassandraBackend, Effect.All] {
    def run(context: CassandraBackend#Context): R = f(context)
    def getDumpInfo = DumpInfo(name = "SimpleCassandraAction")
  }

  trait API extends super.API with ImplicitColumnTypes {
    type SimpleDBIO[+R] = SimpleCassandraAction[R]
    val SimpleDBIO = SimpleCassandraAction
  }

  class SequenceDDL extends super.DDL {
    def createPhase1: Iterable[String] = ???
    def createPhase2: Iterable[String] = ???
    def dropPhase1: Iterable[String] = ???
    def dropPhase2: Iterable[String] = ???
  }

  class TableDDL(val table: Table[_]) extends super.DDL {
    val columns: Iterable[FieldSymbol] = table.create_*
    val tableNode = table.tableNode

    def createPhase1: Iterable[String] = {
      val name = quoteTableName(tableNode)
      val columns = "test int PRIMARY KEY"
      val cql = "CREATE TABLE " + name + "(" + columns + ");"
      println(cql)
      Iterable(cql)
    }
    def createPhase2: Iterable[String] = Iterable()
    def dropPhase1: Iterable[String] = ???
    def dropPhase2: Iterable[String] = ???
  }
}

object CassandraProfile extends CassandraProfile {
}
