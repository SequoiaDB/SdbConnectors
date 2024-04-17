package org.apache.spark.sql.sequoiadb.catalog.read

import com.sequoiadb.spark.{SdbConfig, SdbFilter, SdbPartition}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

/**
 * A factory used to create {@link SdbPartitionReader} instances.
 *
 * If Spark fails to execute any methods in the implementations of this interface or in the returned
 * PartitionReader (by throwing an exception), corresponding Spark task would fail and get retried until
 * hitting the maximum retry times.
 */
case class SdbPartitionReaderFactory(
        config: SdbConfig,
        filter: SdbFilter,
        sourceInfo: String,
        requiredSchema: StructType) extends PartitionReaderFactory {

    /**
     * Returns a row-based partition reader to read data from the given InputPartition.
     *
     * Implementations will cast the input partition to the concrete InputPartition {@link SdbPartition}
     * class defined for the data source.
     *
     * @param partition
     * @return
     */
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
        SdbPartitionReader(
            partition.asInstanceOf[SdbPartition],
            config,
            filter,
            sourceInfo,
            requiredSchema)
    }

}
