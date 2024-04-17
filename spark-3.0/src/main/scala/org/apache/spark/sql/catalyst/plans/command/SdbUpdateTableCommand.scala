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

package org.apache.spark.sql.catalyst.plans.command

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.sequoiadb.catalog.SupportsUpdate
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * SdbUpdateTable's physical plan,
 * It performs an update using assignments on the records that matches condition(filter).
 *
 * @param updatable target table, {@link org.apache.spark.sql.sequoiadb.catalog.SdbTableV2}
 * @param assignments modifier
 * @param filters condition
 */
case class SdbUpdateTableCommand(
        updatable: SupportsUpdate,
        assignments: Seq[Assignment],
        filters: Array[Filter])
    extends RunnableCommand {

    override lazy val output: Seq[Attribute] = Nil

    /**
     * Execution of logical plan {@link SdbUpdateTable}, Here will
     * delegate to {@link SupportsUpdate#updateWhere(Filter[], List<Assignment>)}
     *
     * @param sparkSession
     * @return
     */
    override def run(sparkSession: SparkSession): Seq[Row] = {
        updatable.updateWhere(filters, assignments.asJava)
        Nil
    }

}
