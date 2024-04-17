package org.apache.spark.sql.sequoiadb

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, AttributeReference, Cast, CreateNamedStruct, Expression, ExtractValue, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable

/**
 * Trait with helper functions to generate aligned assignments, even if
 * they are nested fields.
 */
trait AssignmentAlignmentSupport {

    def conf: SQLConf

    private case class ColumnUpdate(ref: Seq[String], expr: Expression)

    /**
     * Align assignments to match table columns.
     * <p>
     * This method processes and reorders given assignments so that each target column gets
     * an expression it should be set to. If a column does not have a matching assignment,
     * it will be set to its current value.
     *
     * For example, if one passes a table with columns c1, c2 and assign c2 = 1, this method
     * will return c1 = c1, c2 = 1
     * <p>
     * This method also handles updates to nested columns. If there is an assignment to
     * particular nested field, this method will construct a new struct with one field updated
     * preserving other fields that have not been modified.
     *
     * For example, if one passes a table with columns c1, c2 where c2 is a struct with fields
     * n1 and n2 and an assignments c2.n2 = 1,
     * this method will return c1 = c1, c2 = struct(c2.n1, 1)
     *
     * @param table the target table
     * @param assignments assignments to align
     * @return aligned assignments that match table columns
     */
    protected def alignAssignments(
            table: LogicalPlan,
            assignments: Seq[Assignment]): Seq[Assignment] = {
        val columnUpdates = assignments.map(a => ColumnUpdate(a.key, a.value))
        val outputExprs = applyUpdates(table.output, columnUpdates)
        outputExprs
            .zip(table.output)
            .map { case (expr, attr) => Assignment(attr, expr) }
    }

    /**
     * Apply column updates (assignments) to schema, generate aligned assignments
     */
    private def applyUpdates(
            cols: Seq[NamedExpression],
            updates: Seq[ColumnUpdate],
            resolver: Resolver = conf.resolver,
            namePrefix: Seq[String] = Nil): Seq[Expression] = {
        // Iterate through columns at the current recursion level and
        // find which column updates match
        cols.map { col =>

            // Find matched column update for current column or any of it children
            val prefixMatchedUpdates = updates.filter(a => resolver(a.ref.head, col.name))
            prefixMatchedUpdates match {

                // if there is no exact match and no match for its children,
                // return the column as is (stop the recursion)
                case updates if updates.isEmpty =>
                    col

                // If there is an exact match, cast if needed and return
                // the assignment expression
                case Seq(update) if isExactMatch(update, col, resolver) =>
                    castIfNeeded(col, update.expr, resolver)

                // If there are matches only for children
                // (StructType will have children)
                case updates if hasExactMatch(updates, col, resolver) =>
                    col.dataType match {
                        case StructType(fields) =>
                            // Build field expression
                            val fieldExprs = fields.zipWithIndex.map { case (field, ordinal) =>
                                Alias(GetStructField(col, ordinal, Some(field.name)), field.name)()
                            }

                            // Recursively apply to this method for nested fields
                            val newUpdates = updates.map(u => u.copy(ref = u.ref.tail))
                            val updateFieldExprs = applyUpdates(fieldExprs, newUpdates, resolver, namePrefix :+ col.name)

                            // Construct a final struct with updated field expressions
                            toNamedStruct(fields, updateFieldExprs)

                        case otherType =>
                            val colName = (namePrefix :+ col.name).mkString(".")
                            throw new AnalysisException(
                                "Updating nested fields is only supported for StructType " +
                                s"but $colName is of type $otherType")
                    }

                // If there are conflicting updates, throw an exception
                // There are two illegal scenarios:
                //
                // - multiple updates to the same column, like "UPDATE `target` SET a = 1, a = 2 WHERE xxx"
                // - updates to a top-level struct and its nested fields (e.g. a.b and a.b.c)
                case updates if hasExactMatch(updates, col, resolver) =>
                    val conflictingCols = updates.map(u => (namePrefix ++ u.ref).mkString("."))
                    throw new AnalysisException(
                        s"Updates are in conflict for these columns: ${conflictingCols.distinct.mkString(", ")}")
            }
        }
    }

    /**
     * Create named struct expression
     */
    private def toNamedStruct(fields: Seq[StructField], fieldExprs: Seq[Expression]): Expression = {
        val namedStructExprs = fields
            .zip(fieldExprs)
            .flatMap { case (field, expr) =>
                Seq(Literal(field.name), expr)
            }
        CreateNamedStruct(namedStructExprs)
    }

    /**
     * Check If there is a column update in updates that matched target column name
     */
    private def hasExactMatch(
            updates: Seq[ColumnUpdate],
            col: NamedExpression,
            resolver: Resolver): Boolean = {
        updates.exists(update =>
            isExactMatch(update, col, resolver))
    }

    /**
     * Check If the column update is exactly matched target column name or not
     */
    private def isExactMatch(
            update: ColumnUpdate,
            col: NamedExpression,
            resolver: Resolver): Boolean = {
        update.ref match {
            case Seq(namePart) if resolver(namePart, col.name) => true
            case _ => false
        }
    }

    /**
     * Add a cast to the child expression if it differs from the specified data type.
     *
     * @param tableAttr the expression to be cast
     * @param expr the target data type
     * @param resolver
     * @return
     */
    protected def castIfNeeded(
            tableAttr: NamedExpression,
            expr: Expression,
            resolver: Resolver): Expression = {
        val storeAssignmentPolicy = conf.storeAssignmentPolicy

        // run the type check and catch type errors
        storeAssignmentPolicy match {
            // strict check
            case StoreAssignmentPolicy.STRICT | StoreAssignmentPolicy.ANSI =>
                if (expr.nullable && !tableAttr.nullable) {
                    throw new AnalysisException(
                        s"Cannot write nullable values to non-null column '${tableAttr.name}'")
                }

                val errors = new mutable.ArrayBuffer[String]()
                // check if data type is compatible
                val canWrite = DataType.canWrite(
                    expr.dataType, tableAttr.dataType, byName = true, resolver, tableAttr.name,
                    storeAssignmentPolicy, err => errors += err)

                if (!canWrite) {
                    throw new AnalysisException(s"Cannot write incompatible data:\n- ${errors.mkString("\n- ")}")
                }
            case _ => // OK
        }

        storeAssignmentPolicy match {
            case _ if tableAttr.dataType.sameType(expr.dataType) =>
                expr
            case StoreAssignmentPolicy.ANSI =>
                AnsiCast(expr, tableAttr.dataType, Option(conf.sessionLocalTimeZone))
            case _ =>
                Cast(expr, tableAttr.dataType, Option(conf.sessionLocalTimeZone))
        }
    }

    /**
     * Transfer a {@link NamedExpression} to full qualified column ref
     *
     * @param expr NamedExpression represents target column to be updated
     */
    implicit protected def asAssignmentReference(expr: Expression): Seq[String] = expr match {
        case attr: AttributeReference => Seq(attr.name)
        case Alias(child, _) => asAssignmentReference(child)
        case GetStructField(child, _, Some(name)) =>
            asAssignmentReference(child) :+ name
        case other: ExtractValue =>
            throw new AnalysisException(s"Updating nested fields is only supported for structs: $other")
        case other =>
            throw new AnalysisException(s"Cannot convert to a reference, unsupported expression: $other")
    }

}
