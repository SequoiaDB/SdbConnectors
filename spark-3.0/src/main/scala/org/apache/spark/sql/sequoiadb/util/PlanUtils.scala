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

package org.apache.spark.sql.sequoiadb.util

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sequoiadb.catalog.SdbTableV2

/**
 * PlanUtils is for {@link LogicalPlan} processing
 */
object PlanUtils {

    /**
     * Check whether logical plan is Sdb ones or not.
     *
     * @param plan
     * @return
     */
    def isSdbRelation(plan: LogicalPlan): Boolean = {
        def isSdbTable(relation: DataSourceV2Relation): Boolean = relation.table match {
            case _: SdbTableV2 => true
            case _ => false
        }

        plan match {
            case s: SubqueryAlias => isSdbRelation(s.child)
            case r: DataSourceV2Relation => isSdbTable(r)
            case _ => false
        }
    }

}
