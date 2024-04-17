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

package com.sequoiadb.sql

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.sequoiadb.{ExtendedDataSourceV2Strategy, PreprocessUpdateTable, RewriteCTAS, SdbAnalysis}

/**
 * An extension for Spark SQL to activate SdbAnalysis for supporting update execution.
 *
 * Examples to enable SdbSparkSessionExtension:
 * 1. spark-sql: ${SPARK_HOME}/bin/spark-sql --conf spark.sql.extensions=com.sequoiadb.sql.SdbSparkSessionExtension
 * 2. thrift-server:  ${SPARK_HOME/sbin/start-thriftserver.sh \
 *      --conf spark.sql.extensions=com.sequoiadb.sql.SdbSparkSessionExtension
 */
class SdbSparkSessionExtension extends (SparkSessionExtensions => Unit) {

    override def apply(extensions: SparkSessionExtensions): Unit = {
        // Inject rule for UpdateTable logical plan processing
        extensions.injectResolutionRule { session => new SdbAnalysis(session) }
        // Inject rule for pre-processing SdbUpdateTable plan
        extensions.injectPostHocResolutionRule(session => PreprocessUpdateTable(session.sessionState.conf))
        extensions.injectPostHocResolutionRule(session => RewriteCTAS(session.sessionState.conf))

        // Inject DSv2 extended strategy
        extensions.injectPlannerStrategy(session => ExtendedDataSourceV2Strategy(session))
    }
}
