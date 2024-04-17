package org.apache.spark.sql.catalyst.plans.command

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}

/**
 * Sdb Create table physical plan
 */
case class SdbCreateTableCommand(
        table: CatalogTable,
        existingTableOpt: Option[CatalogTable],
        mode: SaveMode,
        query: Option[LogicalPlan],
        override val output: Seq[Attribute] = Nil)
    extends RunnableCommand with Logging {

    override def run(sparkSession: SparkSession): Seq[Row] = {
        val table = this.table

        assert(table.tableType != CatalogTableType.VIEW)
        assert(table.identifier.database.isDefined, "Database should have been fixed at analysis")

        val tableExists = existingTableOpt.isDefined
        if (mode == SaveMode.Ignore && tableExists) {
            // Early exit on ignore
            return Nil
        } else if (mode == SaveMode.ErrorIfExists && tableExists) {
            throw new AnalysisException(s"Table ${table.identifier.identifier} already exists")
        }

        require(!tableExists, "Cannot recreate a table when it exists")

        sparkSession.sessionState.catalog.createTable(
            table,
            ignoreIfExists = existingTableOpt.isDefined,
            validateLocation = false)

        Nil
    }

}
