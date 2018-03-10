package com.gravity

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.sql.Row
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types._

/**
  * Created by UNIVERSE on 3/10/18.
  */
class JDBCUtils {



  def getJDBCType(dt: DataType): Option[JdbcType] = dt match {


    case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.BOOLEAN))
    case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
    case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
    case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
    case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
    case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
    case _ => None
  }

  def savePartition(
                   tableName:String,
                   insertStmt:String,
                   conn:Connection,
                   iterator: Iterator[Row],
                   schema:StructType,
                   batchSize:Int


                   ):Unit = {

    //java.sql.Types.CHAR
    val stmt = conn.prepareStatement(insertStmt)
    val setters = schema.fields.map(f => buildSetter(f.dataType))
    val nullTypes = schema.fields.map(f => { getJDBCType(f.dataType).get.jdbcNullType})
    val numFields = schema.fields.length







    try {



        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i)
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }


      Iterator.empty
    } catch {
      case e: Exception => {

        println("EXCEPTION WHILE LOADING DATA")

      }

        throw e


    } finally {
      stmt.close()
      conn.close()
    }



}



  def getInsertStatement(
                        tableName:String,
                        tableSchema:Option[StructType]
                        ):String = {

    val columnNames = tableSchema.get.fieldNames

    val columns = columnNames.mkString(",")
    val placeholders = columnNames.map(_ => "?").mkString(",")
    s"INSERT INTO $tableName ($columns) VALUES ($placeholders)"


  }

  private type ValueSetter = (PreparedStatement, Row, Int) => Unit
  private def buildSetter(

                          dataType: DataType): ValueSetter = dataType match {

    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  //getConnectionFactory


}
