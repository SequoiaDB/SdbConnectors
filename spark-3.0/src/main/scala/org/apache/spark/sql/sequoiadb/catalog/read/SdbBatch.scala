package org.apache.spark.sql.sequoiadb.catalog.read

import com.sequoiadb.spark.{Logging, SdbConfig, SdbFilter, SdbPartitioner}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

/**
 * A physical representation of a data source scan for batch queries. This interface is
 * used to provide physical information,
 *
 * It's about:
 *  1. how many partitions the scanned data has, we have two partition mode (sharding,
 *     datablock).
 *  2. how to read records from the partitions.
 *
 * @param config config for reading sdb collection
 * @param filter query filter
 * @param sourceInfo
 * @param requiredSchema required columns
 */
case class SdbBatch(
        config: SdbConfig,
        filter: SdbFilter,
        sourceInfo: String,
        requiredSchema: StructType)
    extends Batch with Logging {

    /**
     * Returns a list of {@link SdbPartition input partitions}. Each {@link SdbPartition} represents
     * a data split that can be processed by one Spark task. The number of input
     * partitions returned here is the same as the number of RDD partitions this scan outputs.
     * <p>
     * If the {@link Scan} supports filter pushdown, this Batch is likely configured with a filter
     * and is responsible for creating splits for that filter, which is not a full scan.
     * </p>
     * <p>
     * This method will be called only once during a data source scan, to launch one Spark job.
     * </p>
     */
    override def planInputPartitions(): Array[InputPartition] = {
        val partitions = SdbPartitioner(config, filter).computePartitions()
            .asInstanceOf[Array[InputPartition]]

        // log each partition
        partitions.foreach(partition => logInfo(partition.toString))
        partitions
    }

    /**
     * Returns a factory to create a {@link SdbPartitionReader} for reading each {@link SdbPartition}.
     */
    override def createReaderFactory(): PartitionReaderFactory = {
        SdbPartitionReaderFactory(
            config,
            filter,
            sourceInfo,
            requiredSchema)
    }

}
