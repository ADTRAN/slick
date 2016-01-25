package slick.cassandra
import slick.sql.{SqlActionComponent, FixedSqlAction, FixedSqlStreamingAction}
import slick.ast.Node
import slick.dbio.{NoStream, DBIOAction, Effect, SynchronousDatabaseAction}
import slick.SlickException

trait CassandraActionComponent extends SqlActionComponent { self: CassandraProfile =>
  type ProfileAction[+R, +S <: NoStream, -E <: Effect] = FixedSqlAction[R, S, E]
  type StreamingProfileAction[+R, +T, -E <: Effect] = FixedSqlStreamingAction[R, T, E]

  type QueryActionExtensionMethods[R, S <: NoStream] = QueryActionExtensionMethodsImpl[R, S]
  type StreamingQueryActionExtensionMethods[R, T] = StreamingQueryActionExtensionMethodsImpl[R, T]

  class StubAction[+R, +S <: NoStream, -E <: Effect] extends ProfileAction[R, S, E] {
    def overrideStatements(statements: Iterable[String]): slick.sql.SqlAction[R,S,E] = ???
    def statements: Iterable[String] = ???
  }

  abstract class CassandraAction[+R](_name: String, val statements: Vector[String]) extends SynchronousDatabaseAction[R, NoStream, Backend, Effect] with ProfileAction[R, NoStream, Effect] { self =>
    def run(ctx: Backend#Context, sql: Vector[String]): R
    final override def getDumpInfo = super.getDumpInfo.copy(name = _name)
    final def run(ctx: Backend#Context): R = run(ctx, statements)
    final def overrideStatements(_statements: Iterable[String]): ProfileAction[R, NoStream, Effect] = new CassandraAction[R](_name, _statements.toVector) {
      def run(ctx: Backend#Context, sql: Vector[String]): R = self.run(ctx, statements)
    }
  }

  class StreamingStubAction[+R, +S, -E <: Effect] extends StreamingProfileAction[R, S, E] {
    def overrideStatements(statements: Iterable[String]): slick.sql.SqlAction[R,slick.dbio.Streaming[S],E] = ???
    def statements: Iterable[String] = ???
    def head: slick.sql.SqlAction[S,slick.dbio.NoStream,E] = ???
    def headOption: slick.sql.SqlAction[Option[S],slick.dbio.NoStream,E] = ???
  }

  def createQueryActionExtensionMethods[R, S <: NoStream](tree: Node,param: Any): QueryActionExtensionMethods[R,S] =
    new QueryActionExtensionMethodsImpl[R, S](tree, param)

  class QueryActionExtensionMethodsImpl[R, S <: NoStream](tree: Node, param: Any) extends super.QueryActionExtensionMethodsImpl[R, S] {
    def result: ProfileAction[R, S, Effect.Read] =
      new StubAction[R, S, Effect.Read]
  }

  def createStreamingQueryActionExtensionMethods[R, T](tree: Node,param: Any): StreamingQueryActionExtensionMethods[R,T] =
    new StreamingQueryActionExtensionMethodsImpl[R, T](tree, param)

  class StreamingQueryActionExtensionMethodsImpl[R, T](tree: Node, param: Any) extends super.StreamingQueryActionExtensionMethodsImpl[R, T] {
    def result: StreamingProfileAction[R, T, Effect.Read] =
      new StreamingStubAction[R, T, Effect]
  }

  type InsertActionExtensionMethods[T] = CreateInsertActionExtensionMethodsImpl[T]

  def createInsertActionExtensionMethods[T](compiled: CompiledInsert): InsertActionExtensionMethods[T] =
    new CreateInsertActionExtensionMethodsImpl[T](compiled)

  class CreateInsertActionExtensionMethodsImpl[T](compiled: CompiledInsert) extends super.InsertActionExtensionMethodsImpl[T] {
    /** The result type when inserting a single value. */
    type SingleInsertResult = Int

    /** The result type when inserting a collection of values. */
    type MultiInsertResult = Int

    /** An Action that inserts a single value. */
    def += (value: T): ProfileAction[SingleInsertResult, NoStream, Effect.Write] =
      new StubAction[SingleInsertResult, NoStream, Effect.Write]

    /** An Action that inserts a collection of values. */
    def ++= (values: Iterable[T]): ProfileAction[MultiInsertResult, NoStream, Effect.Write] =
      new StubAction[MultiInsertResult, NoStream, Effect.Write]
  }

  type SchemaActionExtensionMethods = SchemaActionExtensionMethodsImpl

  def createSchemaActionExtensionMethods(schema: SchemaDescription): SchemaActionExtensionMethods =
    new SchemaActionExtensionMethodsImpl(schema)

  class SchemaActionExtensionMethodsImpl(schema: SchemaDescription) extends super.SchemaActionExtensionMethodsImpl {
    /** Create an Action that creates the entities described by this schema description. */
    def create: ProfileAction[Unit, NoStream, Effect.Schema] = new CassandraAction[Unit]("schema.create", schema.createStatements.toVector) {
      def run(ctx: Backend#Context, sql: Vector[String]): Unit = {
      }
    }

    /** Create an Action that drops the entities described by this schema description. */
    def drop: ProfileAction[Unit, NoStream, Effect.Schema] = new StubAction[Unit, NoStream, Effect]
  }
}
