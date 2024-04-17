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

package org.apache.spark.sql.sequoiadb.catalog

import com.sequoiadb.base.Sequoiadb
import com.sequoiadb.spark.SdbConfig.mergeGlobalConfs
import com.sequoiadb.spark.{SdbBsonRDD, SdbConfig, SdbConnUtil, SdbException, SdbFilter, SdbModifier, SdbRelation, SdbSchemaSampler}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExtractValue, GetStructField}
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, V1Scan}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsTruncate, V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.sequoiadb.catalog.read.{SdbScan, SdbScanBuilder}
import org.apache.spark.sql.sequoiadb.util.{SdbSourceUtils, SdbUtils}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, DataSourceVersion, SQLContext, SparkSession}

import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, setAsJavaSetConverter}

/**
 * The data source V2 representation of a SequoiaDB table that already exists (Wrapper).
 * With implementation of mix-in interface, like SupportsRead, SupportWrite, SupportUpdate, etc.
 * It can support batch read/write, update, delete records.
 */
case class SdbTableV2(
        spark: SparkSession,
        catalogTable: Option[CatalogTable] = None,
        tableIdent: Option[String] = None,
        tableSchema: Option[StructType] = None,
        options: Map[String, String] = Map.empty)
    extends Table
    with SupportsRead
    with SupportsWrite
    with SupportsUpdate
    with SupportsDelete
    with Logging {

    /**
     * table's name
     */
    override def name(): String = tableIdent.get

    /**
     * parameters of mapping table
     */
    private lazy val parameters: Map[String, String] = {
        var params = options

        if (catalogTable.isDefined) {
            val table = catalogTable.get
            val version = SdbSourceUtils.getDataSourceVersion(table.properties)
            params = version match {
                // v1 properties should get from table storage
                case DataSourceVersion.v1 => Map() ++ table.storage.properties
                case DataSourceVersion.v2 => table.properties
            }
        }

        params
    }

    override def properties(): util.Map[String, String] = parameters.asJava

    /**
     * If user haven't specified the schema for the mapping table
     * Here will use the {@link SdbSchemaSampler} to sample the schema
     */
    private lazy val lazySchema = {
        val config = SdbConfig(parameters)
        val conf: SdbConfig = if (config.samplingSingle) {
            val props = config.properties +
                (SdbConfig.PartitionMode -> SdbConfig.PARTITION_MODE_SINGLE)
            SdbConfig(spark.sqlContext.getAllConfs, props)
        } else {
            config
        }

        val samplingRdd = new SdbBsonRDD(spark.sparkContext, conf,
            numReturned = config.samplingNum)
        new SdbSchemaSampler(samplingRdd, conf.samplingRatio, conf.samplingWithId).sample()
    }

    /**
     * Returns the schema of this table. If the table is not readable and doesn't
     * have a schema will return {@link SdbTableV2.lazySchema}.
     */
    override def schema(): StructType = tableSchema
        .getOrElse(lazySchema)

    /**
     * Returns the set of capabilities for this table.
     */
    override def capabilities(): util.Set[TableCapability] = Set(
        TableCapability.ACCEPT_ANY_SCHEMA,
        TableCapability.BATCH_READ,
        TableCapability.V1_BATCH_WRITE,
        TableCapability.TRUNCATE
    ).asJava

    /**
     * Returns DataSource V2 scan builder which can be used to build a
     * {@link SdbScan}.
     *
     * Spark will call this method to configure each data source scan.
     *
     * @param options The options for reading, which is an immutable
     *                case-insensitive string-to-string map.
     * @return
     */
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        val config = SdbConfig(spark.sqlContext.getAllConfs, parameters)

        SdbConnUtil.setupSessionTimezone(spark, config)
        SdbScanBuilder(
            config,
            SdbConnUtil.generateSourceInfo(spark.sparkContext),
            schema())
    }

    /**
     *
     * @param info
     * @return
     */
    override def newWriteBuilder(info: LogicalWriteInfo)
    : WriteBuilder = {
        new SdbV1WriteBuilder(schema(), parameters)
    }

    /**
     * Checks whether it is possible to delete data from a data source table
     * that matches filter expressions.
     * Spark will call this method at planning time to check whether deleteWhere(Filter[])
     * would reject the delete operation. If this method returns false, Spark will not
     * call deleteWhere(Filter[]).
     *
     * @param filters filter expressions, used to select rows to delete
     *                when all expressions match
     * @return true if the delete operation can be performed
     */
    override def canDeleteWhere(filters: Array[Filter]): Boolean = {
        val filter = SdbFilter(filters)
        if (!filter.unhandledFilters().isEmpty) {
            logWarning(s"Cannot delete records from table: $name(), " +
                s"unhandled filters: ${filter.unhandledFilters().mkString(", ")}")
            return false
        }
        true
    }

    /**
     * Delete data from a data source table that matches filter expressions.
     * Note that this method will be invoked only if canDeleteWhere(Filter[]) returns true.
     *
     * @param filters filter expressions, used to select records to delete
     *                when all expressions match
     */
    override def deleteWhere(filters: Array[Filter]): Unit = {
        val filter = SdbFilter(filters).BSONObj()
        val config = SdbConfig(
            spark.sqlContext.getAllConfs, parameters)

        logInfo(s"Deleting records in table $name(), collection: " +
            s"${config.collectionSpace}.${config.collection}, filter: $filter")

        var sequoiadb: Sequoiadb = null
        try {
            sequoiadb = SdbUtils.create(spark.sparkContext, config)
            val cl = SdbUtils.getCollection(
                sequoiadb, config.collectionSpace, config.collection)

            cl.deleteRecords(filter)
        } finally {
            if (sequoiadb != null) {
                sequoiadb.close()
            }
        }
    }

    /**
     * see {@link SupportsUpdate#canUpdateWhere}
     *
     * @param filters
     */
    override def canUpdateWhere(filters: Array[Filter]): Boolean = {
        val filter = SdbFilter(filters)
        if (filter.unhandledFilters().nonEmpty) {
            logWarning(s"Cannot update records in table: $name(), " +
                s"unhandled filters: ${filter.unhandledFilters().mkString(", ")}")
            return false
        }
        true
    }

    /**
     * Performs update on an external DataSource by filters and assignments.
     */
    override def updateWhere(filters: Array[Filter], assignments: util.List[Assignment]): Unit = {
        val config = SdbConfig(
            spark.sqlContext.getAllConfs, parameters)
        val filter = SdbFilter(filters).BSONObj
        val modifier = SdbModifier(
            assignments, spark.sqlContext.conf.sessionLocalTimeZone)

        if (modifier.unhandled.nonEmpty) {
            throw new SdbException(
                s"Cannot update records in table: $name(), " +
                    s"unhandled assignments: ${modifier.unhandledAssignments.mkString(", ")}")
        }

        logInfo(s"Updating records in table $name(), collection: " +
            s"${config.collectionSpace}.${config.collection}, filter: $filter, modifier: $modifier")

        var sequoiadb: Sequoiadb = null
        try {
            sequoiadb = SdbUtils.create(spark.sparkContext, config)
            val cl = SdbUtils.getCollection(
                sequoiadb, config.collectionSpace, config.collection)

            cl.updateRecords(
                filter,
                modifier.bsonModifier)
        } finally {
            if (sequoiadb != null) {
                sequoiadb.close()
            }
        }
    }

    implicit protected def asAssignmentReference(expr: Expression): Seq[String] = expr match {
        case attr: AttributeReference => Seq(attr.name)
        case Alias(child, _) => asAssignmentReference(child)
        case GetStructField(child, _, Some(name)) =>
            asAssignmentReference(child) :+ name
        case other: ExtractValue =>
            throw new AnalysisException(s"Updating nested fields is only supported for structs: $other")
        case other =>
            throw new AnalysisException(s"Cannot convert to a reference, unsupported expression: $other")
    }

}

// ================ V1 Fallbacks ==============

/**
 * A fallback for V1 DataSource that would like to leverage the DataSource V2 read code paths.
 *
 * This interface is designed to provide Spark DataSources time to migrate to DataSource V2
 * and will be removed in a future release.
 *
 * @param schema
 * @param scanOptions
 */
private class SdbV1ScanBuilder(
        schema: StructType,
        scanOptions: Map[String, String])
    extends ScanBuilder with Logging {

    /**
     * Create an `BaseRelation` with `TableScan` that can scan data from DataSource v1
     * to RDD[Row].
     */
    override def build(): Scan = new V1Scan {
        override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext)
        : T = {
            val session = SparkSession.active.sqlContext
            SdbRelation(
                session,
                mergeGlobalConfs(session.getAllConfs, scanOptions),
                Option(schema)).asInstanceOf[T]
        }

        override def readSchema(): StructType = schema
    }
}

/**
 * A fallback for V1 DataSources that would like to leverage the DataSource V2 write code paths.
 * The InsertableRelation will be used only to Append data.
 *
 * This interface is designed to provide Spark DataSources time to migrate to DataSource V2
 * and will be removed in a future release.
 *
 * @param schema
 * @param writeOptions
 */
private class SdbV1WriteBuilder(
        schema: StructType,
        writeOptions: Map[String, String])
    extends V1WriteBuilder with SupportsTruncate with Logging {

    private var forceOverwrite = false

    /**
     * Creates an InsertableRelation (V1 API) that allows appending a DataFrame
     * to destination (Sdb).
     */
    override def buildForV1Write(): InsertableRelation = {
        val session = SparkSession.active.sqlContext

        val config = SdbConfig(session.getAllConfs, writeOptions)
        var sequoiadb: Sequoiadb = null
        try {
            sequoiadb = SdbUtils.create(
                session.sparkContext, config)

            // ensure collection - create collection space and collection if they are not exist.
            SdbUtils.ensureCollection(
                sequoiadb, config.collectionSpace, config.collection, Option(config))

            if (forceOverwrite) {
                val cl = SdbUtils.getCollection(
                    sequoiadb, config.collectionSpace, config.collection)

                logInfo(s"Truncate collection ${config.collectionSpace}.${config.collection} " +
                    s"before insert overwrite.")
                cl.truncate()
            }
        } finally {
            if (sequoiadb != null) {
                sequoiadb.close()
            }
        }

        SdbRelation(
            session,
            mergeGlobalConfs(session.getAllConfs, writeOptions),
            Option(schema))
    }

    override def truncate(): WriteBuilder = {
        forceOverwrite = true
        this
    }

}