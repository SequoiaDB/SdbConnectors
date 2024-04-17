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

import org.apache.spark.sql.DataSourceVersion

import java.util.Locale

/**
 * Data Source V2 Util
 */
object SdbSourceUtils {

    val NAME = "sequoiadb"
    val ALT_NAME = "com.sequoiadb.spark"

    /**
     * Check if a spark mapping table is a Sdb Table
     */
    def isSdbTable(provider: Option[String]): Boolean = {
        provider.exists(isSdbDataSourceName)
    }

    /**
     * Check if data source name is Sdb Data Source name
     */
    def isSdbDataSourceName(name: String): Boolean = {
        name.toLowerCase(Locale.ROOT) == NAME ||
        name.toLowerCase(Locale.ROOT) == ALT_NAME
    }

    /**
     * Get data source api version from table properties.
     * (v1 will be returned by default)
     */
    def getDataSourceVersion(properties: Map[String, String]): String = {
        properties.getOrElse(DataSourceVersion.OPTION_NAME, DataSourceVersion.v1)
    }

}
