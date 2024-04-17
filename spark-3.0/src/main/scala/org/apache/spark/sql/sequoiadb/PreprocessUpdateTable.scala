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

package org.apache.spark.sql.sequoiadb

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.command.SdbUpdateTableCommand
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, LogicalPlan, SdbUpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sequoiadb.catalog.SupportsUpdate
import org.apache.spark.sql.sources.Filter

/**
 * Preprocess the {@link SdbUpdateTable} logical plan before converting it to physical
 * plan {@link SdbUpdateTableCommand}.
 *
 * Here will do things:
 *
 * - Check and transform the update condition (filters)
 * - Align assignments
 * - Transform logical plan to physical plan
 */
case class PreprocessUpdateTable(sqlConf: SQLConf)
    extends Rule[LogicalPlan] with AssignmentAlignmentSupport with PredicateHelper {

    import SdbDataSourceV2Implicits._

    override def conf: SQLConf = sqlConf

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
        case u: SdbUpdateTable if u.resolved =>
            u.child match {
                case r @ DataSourceV2Relation(table, output, _, _, _) =>
                    // process update condition
                    val cond = u.condition
                    if (cond.exists(SubqueryExpression.hasSubquery)) {
                        throw new AnalysisException(
                            s"Update by condition with sub query is not supported: $cond")
                    }
                    // fail if any filter cannot be converted.
                    // correctness depends on updating all matching data.
                    val filters = DataSourceStrategy.normalizeExprs(cond.toSeq, output)
                        .flatMap(splitConjunctivePredicates(_).map {
                            filter => DataSourceStrategy
                                .translateFilter(filter, supportNestedPredicatePushdown = true)
                                .getOrElse(
                                    throw new AnalysisException(s"Exec update failed:" +
                                        s" cannot translate expression to target filter: $filter"))
                        }).toArray

                    if (!table.asUpdatable.canUpdateWhere(filters)) {
                        throw new AnalysisException(s"Cannot update table ${table.name} by filter:" +
                            s" ${filters.mkString("[", ",", "]")}")
                    }

                    toPhysical(table.asUpdatable, u.assignments, filters)

                case _ =>
                    throw new AnalysisException(s"UPDATE is only supported with data source V2 tables.")
            }
    }

    /**
     * Transform logical plan to physical plan
     */
    private def toPhysical(
            updatable: SupportsUpdate,
            assignments: Seq[Assignment],
            filters: Array[Filter]): RunnableCommand = {
        SdbUpdateTableCommand(updatable, assignments, filters)
    }

}
