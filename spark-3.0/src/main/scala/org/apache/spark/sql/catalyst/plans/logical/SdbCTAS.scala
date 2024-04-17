package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

/**
 * Logical plan that perform CTAS(Create table as select) for Sequoiadb Data Source.
 */
case class SdbCTAS(
        catalog: TableCatalog,
        tableName: Identifier,
        partitioning: Seq[Transform],
        query: LogicalPlan,
        properties: Map[String, String],
        writeOptions: Map[String, String],
        ignoreIfExists: Boolean) extends Command with V2CreateTablePlan {

    override def tableSchema: StructType = query.schema
    override def children: Seq[LogicalPlan] = Seq(query)

    override lazy val resolved: Boolean = childrenResolved && {
        // the table schema is created from the query schema, so the only resolution needed is to check
        // that the columns referenced by the table's partitioning exist in the query schema
        val references = partitioning.flatMap(_.references).toSet
        references.map(_.fieldNames).forall(query.schema.findNestedField(_).isDefined)
    }

    override def withPartitioning(rewritten: Seq[Transform]): V2CreateTablePlan = {
        this.copy(partitioning = rewritten)
    }

}
