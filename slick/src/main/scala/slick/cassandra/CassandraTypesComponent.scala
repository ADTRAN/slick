package slick.cassandra
import slick.relational.RelationalTypesComponent
import slick.ast.{NumericTypedType, BaseTypedType}

trait CassandraTypesComponent extends RelationalTypesComponent { self: CassandraProfile =>
  object MappedCassandraType extends MappedColumnTypeFactory {
    import scala.reflect.ClassTag
    def base[T : ClassTag, U : BaseColumnType](tmap: T => U, tcomap: U => T): BaseColumnType[T] = ???
  }

  trait CassandraType[T] extends BaseTypedType[T] {
    def classTag: scala.reflect.ClassTag[_] = ???
    def scalaType: slick.ast.ScalaType[T] = ???
  }

  object CassandraBigDecimalColumnType extends CassandraType[BigDecimal] with NumericTypedType
  object CassandraBooleanColumnType extends CassandraType[Boolean]
  object CassandraByteColumnType extends CassandraType[Byte] with NumericTypedType
  object CassandraCharColumnType extends CassandraType[Char]
  object CassandraDoubleColumnType extends CassandraType[Double] with NumericTypedType
  object CassandraFloatColumnType extends CassandraType[Float] with NumericTypedType
  object CassandraIntColumnType extends CassandraType[Int] with NumericTypedType
  object CassandraLongColumnType extends CassandraType[Long] with NumericTypedType
  object CassandraShortColumnType extends CassandraType[Short] with NumericTypedType
  object CassandraStringColumnType extends CassandraType[String]

  trait ImplicitColumnTypes extends super.ImplicitColumnTypes {
    implicit def bigDecimalColumnType = CassandraBigDecimalColumnType
    implicit def booleanColumnType = CassandraBooleanColumnType
    implicit def byteColumnType = CassandraByteColumnType
    implicit def charColumnType = CassandraCharColumnType
    implicit def doubleColumnType = CassandraDoubleColumnType
    implicit def floatColumnType = CassandraFloatColumnType
    implicit def intColumnType = CassandraIntColumnType
    implicit def longColumnType = CassandraLongColumnType
    implicit def shortColumnType = CassandraShortColumnType
    implicit def stringColumnType = CassandraStringColumnType
  }
}
