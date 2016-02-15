package slick.cassandra

import slick.ast.{BaseTypedType, Type, ScalaBaseType}
import scala.reflect.ClassTag
import slick.SlickException

/** A CassandraType object represents a Scala type that can be used as a column type in the database. */
trait CassandraType[@specialized(Int) T] extends BaseTypedType[T] { self =>
  /** The default name for the CQL type that is used for column declarations. */
  def cqlTypeName: String

  override def toString = scalaType.toString + "'"
}

abstract class DriverCassandraType[@specialized T](implicit val classTag: ClassTag[T]) extends CassandraType[T] {
  def scalaType = ScalaBaseType[T]
}

class IntCassandraType extends DriverCassandraType[Int] {
  def cqlTypeName = "int"
}

object CassandraType {
  def unapply(t: Type) = Some(cassandraTypeFor(t))

  val intCassandraType = new IntCassandraType

  def cassandraTypeFor(t: Type): CassandraType[Any] = ((t.structural match {
    case CassandraProfile.CassandraIntColumnType => intCassandraType
    case t                                       => throw new SlickException("CassandraProfile has no CassandraType for type " + t)
  }): CassandraType[_]).asInstanceOf[CassandraType[Any]]
}
