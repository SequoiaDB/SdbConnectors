package org.apache.spark.sql.catalyst.plans.command

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfoImpl, SupportsTruncate, V1WriteBuilder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{SupportsV1Write, V2TableWriteExec}
import org.apache.spark.sql.sequoiadb.SdbDataSourceV2Implicits
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils
import com.sequoiadb.spark.SdbException
import org.apache.spark.sql.AnalysisException

import java.util.UUID
import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * Sdb CTAS physical plan, code is copied from [[CreateTableAsSelectExec]]
 */
case class SdbCreateTableAsSelectExec(
        catalog: TableCatalog,
        ident: Identifier,
        partitioning: Seq[Transform],
        plan: LogicalPlan,
        query: SparkPlan,
        properties: Map[String, String],
        writeOptions: CaseInsensitiveStringMap,
        ifNotExists: Boolean) extends TableWriteExecHelper {

    override protected def run(): Seq[InternalRow] = {
        if (catalog.tableExists(ident)) {
            if (ifNotExists) {
                return Nil
            }

            throw new TableAlreadyExistsException(ident)
        }

        val schema = CharVarcharUtils.getRawSchema(query.schema).asNullable
        val table = catalog.createTable(ident, schema,
            partitioning.toArray, properties.asJava)
        writeToTable(catalog, table, writeOptions, ident)
    }
}

trait TableWriteExecHelper extends V2TableWriteExec with SupportsV1Write {
    import SdbDataSourceV2Implicits._
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper

    protected def writeToTable(
            catalog: TableCatalog,
            table: Table,
            writeOptions: CaseInsensitiveStringMap,
            ident: Identifier): Seq[InternalRow] = {
        Utils.tryWithSafeFinallyAndFailureCallbacks({
            table match {
                case table: SupportsWrite =>
                    val info = LogicalWriteInfoImpl(
                        queryId = UUID.randomUUID().toString,
                        query.schema,
                        writeOptions)
                    val writeBuilder = table.newWriteBuilder(info)

                    logInfo(s"Table will be truncate before CTAS.")
                    val writtenRows = writeBuilder match {
                        case v1: V1WriteBuilder =>
                            /**
                             * Here will enable truncate before CTAS that performs
                             * overwrite.
                             * For now, we haven't implemented V2 Write API for Sdb
                             * Connector, so writes will fall back to v1 implementation.
                             *
                             * Notes:
                             *  - here is only for v2 physical plan.
                             */
                            writeWithV1(
                                v1.asSupportsTruncate.truncate().asV1Builder.buildForV1Write())
                        case _ =>
                            // TODO: will support in the future
                            throw new AnalysisException(
                                s"table does not support v2 write temporarily. " +
                                    s"name: ${ident.namespace().mkString(".")}.${ident.name()}")
                    }

                    table match {
                        case st: StagedTable => st.commitStagedChanges()
                        case _ =>
                    }
                    writtenRows

                case _ =>
                    // Table does not support writes - staged changes are also rolled back below if table
                    // is staging.
                    throw new SparkException(
                        s"Table implementation does not support writes: ${ident.quoted}")
            }
        })(catchBlock = {
            table match {
                // Failure rolls back the staged writes and metadata changes.
                case st: StagedTable => st.abortStagedChanges()
                case _ => catalog.dropTable(ident)
            }
        })
    }
}