/*
 * Copyright 2022 SequoiaDB Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.sequoiadb.spark

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.types._
import org.bson.types.{BSONDate, BSONDecimal, BSONTimestamp}
import org.bson.{BSONObject, BasicBSONObject}

import java.time._
import scala.collection.mutable.ArrayBuffer

/**
 * Sdb update modifier
 *
 * @param modifier which supports $set, $field
 * @param unhandled assignments that cannot be handled by Sdb
 */
case class SdbModifier(
        modifier: BSONObject,
        unhandled: Array[Assignment],
        sessionTimeZone: String) extends Serializable {

    def bsonModifier: BSONObject = modifier

    def unhandledAssignments: Array[Assignment] = unhandled

    override def toString: String = modifier.toString

}

/**
 * Util functions for SdbModifier generating and processing
 */
object SdbModifier {

    /**
     * microseconds per millisecond
     */
    val MICROSECONDS_PER_MILLISECOND = 1000000

    /**
     * session timezone, get from spark conf `spark.sql.session.timeZone`
     */
    var SESSION_TIME_ZONE = ZoneId.systemDefault()

    /**
     * supported modifier
     */
    val $SET = "$set"
    val $FIELD = "$field"

    def apply(assignments: java.util.List[Assignment],
              sessionTimeZone: String)
    : SdbModifier = {
        SESSION_TIME_ZONE = ZoneId
            .of(sessionTimeZone)
        val (m, u) = toBSONModifier(assignments)
        new SdbModifier(m, u, sessionTimeZone)
    }

    /**
     * translate Spark SQL's assignments {@link Assignment} to Sdb Modifier.
     *
     * @param assignments
     */
    def toBSONModifier(assignments: java.util.List[Assignment]): (BSONObject, Array[Assignment]) = {
        val modifier = new BasicBSONObject()
        val unhandled = ArrayBuffer[Assignment]()

        val setter = new BasicBSONObject()
        assignments.forEach { assignment =>
            /**
             * Assignment is a key-value structure
             *  1. its key is a NamedExpression, tells that which column is to update
             *  2. its values is a ValueExpression that is to be set.
             *
             * Here will transfer it to BSON Modifier
             */
            val ref = asModifierRef(assignment.key)
            val value = asModifierVal(assignment.value)

            // If ref or value is empty or none, that means Sdb cannot handle
            // this assignment, we need to add it to unhandled array.
            if (ref.isEmpty || value == None) {
                unhandled += assignment
            }

            setter.put(ref.mkString("."), value)
        }

        modifier.put($SET, setter)

        // After that, return modifier and unhandled assignments
        (modifier, unhandled.toArray)
    }

    /**
     * Translate {@link NamedExpression} to full qualified column ref
     *
     * @param expr named expression represents target column
     *             which is to be updated
     * @return full qualified column name separate in Seq
     */
    implicit def asModifierRef(expr: Expression): Seq[String] = expr match {
        case attr: AttributeReference => Seq(attr.name)
        case Alias(child, _) => asModifierRef(child)
        case GetStructField(child, _, Some(name)) => asModifierRef(child) :+ name

        // updating nested fields is only supported by StructType
        case _ => Seq()
    }

    /**
     * Translate ValueExpression to Sdb column value.
     *
     * @param expr
     * @return
     */
    implicit def asModifierVal(expr: Expression): Any = expr match {
        // processing literal assignment
        case lit: Literal =>
            (lit.value, lit.dataType) match {
                // The internal type of Char, Varchar, String is UTF8String
                // Here should cast to Java String.
                // Otherwise, Sdb BSON library can not serialize it.
                case (v, _: CharType) => v.toString
                case (v, _: VarcharType) => v.toString
                case (v, _: StringType) => v.toString

                // The internal type of DateType is Integer, it represents
                // the epoch day of 1970-01-01, and it valid range is [0001-01-01, 9999-12-31].
                case (v: Int, _: DateType) =>
                    val instant = LocalDate.ofEpochDay(v)
                        .atStartOfDay(ZoneId.systemDefault())
                        .toInstant
                    val ldt = LocalDateTime.ofInstant(instant, SESSION_TIME_ZONE)

                    BSONDate.valueOf(ldt)

                // The internal type of TimestampType is Long, it represents
                // a time instant in microsecond precision.
                // Epoch millisecond of 1970-01-01T00:00:00.00000Z (UTC +00:00).
                case (v: Long, _: TimestampType) =>
                    // transform microseconds to milliseconds
                    val time = (v / MICROSECONDS_PER_MILLISECOND).toInt
                    val inc = (v % MICROSECONDS_PER_MILLISECOND).toInt

                    new BSONTimestamp(time, inc)

                case (v: Decimal, _: DecimalType) =>
                    new BSONDecimal(v.toJavaBigDecimal)

                case (v, _) => v
            }

        // Assign another column's value to target column
        case attr: AttributeReference =>
            val fieldRef = new BasicBSONObject()

            val ref = asModifierRef(attr)
            if (ref.isEmpty) {
                return None
            }

            fieldRef.put($FIELD, ref.mkString("."))
            fieldRef

        case _ => None
    }

}
