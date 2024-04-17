package org.apache.spark.sql.sequoiadb.catalog.read

import com.sequoiadb.spark.{SdbConfig, SdbFilter}
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

/**
 * A logical representation of a data source scan. This interface is used to provide logical information,
 * like what the actual read schema is.
 */
case class SdbScan(
        config: SdbConfig,
        filter: SdbFilter,
        sourceInfo: String,
        requiredSchema: StructType)
    extends Scan {

    /**
     * Returns the actual schema of this data source scan, which may be different from the physical
     * schema of the underlying storage, as column pruning or other optimizations may happen.
     *
     * @return actual schema of current query
     */
    override def readSchema(): StructType = requiredSchema

    override def description(): String = "SequoiaDB Scan"

    /**
     * Returns the physical representation of this scan for batch query.
     *
     * @return
     */
    override def toBatch: Batch = {
        SdbBatch(config, filter, sourceInfo, requiredSchema)
    }

}
