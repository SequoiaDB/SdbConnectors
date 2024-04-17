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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SdbUpdateTable, SubqueryAlias, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sequoiadb.util.PlanUtils.isSdbRelation

/**
 * Analysis rules for SequoiaDB.
 * Currently, only add a rule for supporting to update Sdb Table.
 */
class SdbAnalysis(session: SparkSession)
    extends Rule[LogicalPlan] with AssignmentAlignmentSupport with PredicateHelper with Logging {

    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
        case u @ UpdateTable(table, assignments, cond) =>
            // Get DataSourceV2 Relation from UpdateTable plan
            val relation = table match {
                case s: SubqueryAlias =>
                    s.child
                case r: DataSourceV2Relation => r
            }

            // Check if it's a Sdb Relation, then transform to SdbUpdateTable Plan
            if (isSdbRelation(relation))
                SdbUpdateTable(relation, assignments, cond)
            // Otherwise, return the original plan
            else u
    }

}
