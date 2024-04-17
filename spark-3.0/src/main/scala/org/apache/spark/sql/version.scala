package org.apache.spark.sql

/**
 * This object is for managing Spark Data Source info.
 *
 * It's as an option stored in table properties, those table which are created by v1 api
 * doesn't has such an option (but it will return v1 by default).
 */
object DataSourceVersion {
    /**
     * Currently only support v1, v2
     */
    val v1: String = "v1"
    val v2: String = "v2"

    val OPTION_NAME: String = "$SEQUOIADB_SPARK_DATA_SOURCE_API_VERSION"

    /**
     * If we upgrade data source in the future, we should change it.
     */
    val CURRENT_VERSION: String = v2
}
