package org.apache.spark.sql.sequoiadb.catalog.read

import com.sequoiadb.base.{DBCollection, Sequoiadb}
import com.sequoiadb.spark.{BSONConverter, Logging, PartitionMode, SdbConfig, SdbConnUtil, SdbCursor, SdbFastCursor, SdbFilter, SdbPartition}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sequoiadb.util.SdbUtils
import org.apache.spark.sql.types.StructType
import org.bson.types.BasicBSONList
import org.bson.{BSONObject, BasicBSONObject}

import scala.collection.JavaConverters._

/**
 * A partition reader for SequoiaDB.
 * A partition cloud be a bunch of datablocks(extents), or a sharding of a collection.
 *
 * See also: {@link SdbPartition}
 *
 * @param partition
 * @param config
 * @param filter
 * @param sourceInfo
 * @param requiredSchema
 */
case class SdbPartitionReader(
        partition: SdbPartition,
        config: SdbConfig,
        filter: SdbFilter,
        sourceInfo: String,
        requiredSchema: StructType)
    extends PartitionReader[InternalRow]
    with Serializable
    with Logging {

    BSONConverter.SESSION_TIMEZONE = config.sessionTimezone

    /**
     * create sequoiadb connection
     */
    private val sequoiadb: Sequoiadb = {
        val conn = new Sequoiadb(
            partition.urls.asJava,
            config.username,
            config.password,
            SdbConfig.SdbConnectionOptions)

        // setup source info
        SdbConnUtil.setupSourceSessionAttrIgnoreFailures(conn, sourceInfo)
        conn
    }

    /**
     * hint info for datablock partition mode.
     */
    private val hint: BSONObject = {
        val hintObj = new BasicBSONObject()

        if (partition.mode == PartitionMode.Datablock) {
            val datablocks = new BasicBSONList()
            partition.blocks.foreach(blockId =>
                datablocks.add(blockId.asInstanceOf[Integer]))

            val metaObj = new BasicBSONObject()
            // set up which datablocks(extents) should be read by current task(partition)
            metaObj.put("Datablocks", datablocks)
            metaObj.put("ScanType", "tbscan")

            hintObj.put("$Meta", metaObj)
        }

        hintObj
    }

    // create selector for pruning columns
    private val selector: BSONObject = SdbPartitionReader.createSelector(requiredSchema)

    logInfo(s"selector=$selector")
    logInfo(s"filter=${filter.BSONObj()}")

    private lazy val fastCursor: SdbCursor  = {
        val cl: DBCollection =
            SdbUtils.getCollection(sequoiadb, partition.csName, partition.clName)

        val cursor = cl.query(
            // Limit, offset is set to 0, -1,
            // because Limit/Offset push down is not supported in Spark 3.1.2 yet.
            // TODO: need to supports these push down when we upgrade Spark to 3.3.x
            partition.filter.BSONObj(), selector, null, hint, 0, -1, 0)

        if (log.isDebugEnabled) {
            logDebug(s"create fast cursor, " +
                     s"buf size = ${config.fastCursorBufSize}, " +
                     s"decoder num = ${config.fastCursorDecoderNum}")
        }
        new SdbFastCursor(
            cursor, config.fastCursorBufSize, config.fastCursorDecoderNum)
    }

    /**
     * Proceed to next record, returns false if there is no more records.
     *
     * @throws IOException if failure happens during disk/network IO like reading files.
     */
    override def next(): Boolean = fastCursor.hasNext

    /**
     * Return the current record. This method should return same value until `next` is called.
     */
    override def get(): InternalRow = {
        val record = fastCursor.next()
        // convert bson to spark internal row.
        BSONConverter.bsonToRow(record, requiredSchema)
    }

    /**
     * close connection/cursor to SequoiaDB.
     */
    override def close(): Unit = {
        if (fastCursor != null) {
            fastCursor.close()
        }
        if (sequoiadb != null) {
            sequoiadb.close()
        }
    }

}

object SdbPartitionReader {

    /**
     * create selector, structure like {"c1": null, "c2": null}
     *
     * @param requiredSchema Spark struct type
     * @return
     */
    def createSelector(requiredSchema: StructType): BSONObject = {
        val selector = new BasicBSONObject
        requiredSchema.names.map {
            selector.put(_, null)
        }

        selector
    }
}