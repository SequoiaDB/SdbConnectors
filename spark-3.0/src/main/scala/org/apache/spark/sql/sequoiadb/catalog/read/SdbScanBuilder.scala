package org.apache.spark.sql.sequoiadb.catalog.read

import com.sequoiadb.spark.{SdbConfig, SdbFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * An builder for building the {@link SdbScan}. It can mix-in SupportsPushDownXYZ
 * interfaces to do operator pushdown.
 *
 * For now, we only support project, filter push down.
 */
case class SdbScanBuilder(
        config: SdbConfig,
        sourceInfo: String,
        var schema: StructType)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with Logging {

    // default filter is an empty selector -> {} (empty bson object)
    var filter: SdbFilter = SdbFilter()

    override def build(): Scan = SdbScan(
        config,
        filter,
        sourceInfo,
        schema)

    /**
     * Applies column pruning (projection).
     */
    override def pruneColumns(requiredSchema: StructType): Unit = {
        schema = requiredSchema
    }

    /**
     * Pushes down filters, and returns filters that need to be evaluated after scanning.
     * Rows should be returned from the data source if and only if all of the filters match.
     * That is, filters must be interpreted as ANDed together.
     *
     * @param filters
     * @return unhandled filters
     */
    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
        filter = SdbFilter(filters)
        filter.unhandledFilters()
    }

    override def pushedFilters(): Array[Filter] = Array()

}
