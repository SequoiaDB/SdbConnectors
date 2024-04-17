package org.apache.spark.sql.sequoiadb

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.command.SdbCreateTableAsSelectExec
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SdbCTAS}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, StagingTableCatalog}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, SparkSession, Strategy}

import scala.collection.JavaConverters.mapAsJavaMapConverter

case class ExtendedDataSourceV2Strategy(session: SparkSession) extends Strategy with Logging {

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case SdbCTAS(catalog, ident, parts, query, props, options, ifNotExists) =>
            val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
            val writeOptions = new CaseInsensitiveStringMap(options.asJava)
            catalog match {
                case _: StagingTableCatalog =>
                    throw new AnalysisException(
                        s"table ${ident.namespace().mkString(".")}.${ident.name()} does not support staging.")
                case _ =>
                    SdbCreateTableAsSelectExec(catalog, ident, parts, query, planLater(query),
                        propsWithOwner, writeOptions, ifNotExists) :: Nil
            }

        case _ => Nil
    }


}
