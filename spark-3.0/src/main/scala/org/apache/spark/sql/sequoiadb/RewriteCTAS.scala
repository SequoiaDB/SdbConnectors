package org.apache.spark.sql.sequoiadb

import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan, SdbCTAS}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sequoiadb.util.SdbSourceUtils

/**
 * Rewrite Logical Plan [[CreateTableAsSelect]] if target table is a sdb table.
 *
 * - [[CreateTableAsSelect]] transforms into [[SdbCTAS]]
 *
 * @param sqlConf
 */
case class RewriteCTAS(sqlConf: SQLConf) extends Rule[LogicalPlan]  {

    override def conf: SQLConf = sqlConf

    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
        case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists)
                if SdbSourceUtils.isSdbTable(props.get(TableCatalog.PROP_PROVIDER)) =>
            SdbCTAS(catalog, ident, parts, query, props, options, ifNotExists)
    }

}
