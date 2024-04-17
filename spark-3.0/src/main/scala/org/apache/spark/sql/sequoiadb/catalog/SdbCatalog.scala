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

import com.sequoiadb.spark.SdbException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.command.SdbCreateTableCommand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sequoiadb.util.SdbSourceUtils
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, DataSourceVersion, SaveMode, SparkSession}

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

/**
 * A Catalog extension which can properly handle the interaction between the HiveMetaStore and
 * Sdb data source V2 tables.
 * It delegates all DataSources other than Sdb to the SparkCatalog.
 */
class SdbCatalog extends DelegatingCatalogExtension
    with DataSourceRegister
    with TableProvider
    with Logging {

    val spark = SparkSession.active

    override def createTable(
            ident: Identifier,
            schema: StructType,
            partitions: Array[Transform],
            properties: util.Map[String, String])
    : Table = {
        if (SdbSourceUtils.isSdbDataSourceName(getProvider(properties))) {
            logInfo(s"Creating sdb table, " +
                s"name: ${ident.namespace().mkString(".")}.${ident.name()}, schema: $schema")

            createSdbTable(
                ident,
                schema,
                partitions,
                properties,
                Map.empty,
                sourceQuery = None)
        } else {
            super.createTable(ident, schema, partitions, properties)
        }
    }

    /**
     * Create a Sdb CatalogTable
     *
     * @param ident Identifier of table, consists of
     *              database name and table name
     * @param schema Schema of the table
     * @param partitions Partition transforms for the table
     *                   Currently is not supported
     * @param allTableProperties Table properties from OPTIONS clause
     * @param writeOptions Options specific to the write operation
     * @return
     */
    private def createSdbTable(
            ident: Identifier,
            schema: StructType,
            partitions: Array[Transform],
            allTableProperties: util.Map[String, String],
            writeOptions: Map[String, String],
            sourceQuery: Option[DataFrame])
    : Table = {
        val (partitionColumns, bucketSpecOpt) = convertTransforms(partitions)
        val tableId = TableIdentifier(ident.name(), ident.namespace().lastOption)
        val existingTableOpt = getExistingTableIfExists(tableId)
        val storage = DataSource.buildStorageFormatFromOptions(writeOptions)
        val withVersion = Map(
            DataSourceVersion.OPTION_NAME -> DataSourceVersion.CURRENT_VERSION) ++ allTableProperties.asScala.toMap


        val tableDesc = new CatalogTable(
            identifier = tableId,
            tableType = CatalogTableType.MANAGED,
            storage = storage,
            schema = schema,
            provider = Some(SdbSourceUtils.NAME),
            partitionColumnNames = partitionColumns,
            bucketSpec = bucketSpecOpt,
            properties = withVersion,
            comment = None)

        val withDb = verifyTableAndSolidify(tableDesc, None)

        SdbCreateTableCommand(
            withDb,
            existingTableOpt,
            SaveMode.ErrorIfExists,
            None).run(spark)

        loadTable(ident)
    }

    /**
     * Verify table and solidify
     */
    private def verifyTableAndSolidify(
            tableDesc: CatalogTable,
            query: Option[LogicalPlan]): CatalogTable = {
        // Fow now, we don't support table partitioning and bucketing.
        if (tableDesc.bucketSpec.isDefined) {
            val ident = tableDesc.identifier
            throw new SdbException(s"Bucketing is not supported not for ${ident.database}.${ident.table}.")
        }

        val schema = query.map { plan =>
            assert(tableDesc.schema.isEmpty, "Cannot specify table schema in CTAS.")
            plan.schema.asNullable
        }.getOrElse(tableDesc.schema)

        PartitioningUtils.validatePartitionColumn(
            schema,
            tableDesc.partitionColumnNames,
            caseSensitive = false)

        // Add database info into CatalogTable
        val catalog = spark.sessionState.catalog
        val db = tableDesc.identifier.database
            .getOrElse(catalog.getCurrentDatabase)
        val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))

        tableDesc.copy(
            identifier = tableIdentWithDB,
            schema = schema)
    }

    /**
     * Get existing table in catalog, return None if table is not exist.
     */
    private def getExistingTableIfExists(tableId: TableIdentifier): Option[CatalogTable] = {
        val catalog = spark.sessionState.catalog

        val tableExists = catalog.tableExists(tableId)
        if (tableExists) {
            val oldTable = catalog.getTableMetadata(tableId)
            // VIEW is read-only
            if (oldTable.tableType == CatalogTableType.VIEW) {
                throw new SdbException(s"Cannot write into view ${tableId.database}.${tableId.table}")
            }
            if (!SdbSourceUtils.isSdbTable(oldTable.provider)) {
                throw new SdbException(s"${tableId.database}.${tableId.table} is not a sequoiadb table")
            }

            Some(oldTable)
        } else {
            None
        }
    }

    /**
     * Convert v2 Transforms to v1 partition columns and an optional bucket spec.
     */
    private def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
        val identityCols = new mutable.ArrayBuffer[String]
        var bucketSpec = Option.empty[BucketSpec]

        partitions.map {
            case IdentityTransform(FieldReference(Seq(col))) =>
                identityCols += col

            case BucketTransform(numBuckets, FieldReference(Seq(col))) =>
                bucketSpec = Some(BucketSpec(numBuckets, col :: Nil, Nil))

            case transform =>
                throw new UnsupportedOperationException(
                    s"SessionCatalog does not support partition transform: $transform")
        }

        (identityCols.toSeq, bucketSpec)
    }

    override def loadTable(ident: Identifier): Table = {
        super.loadTable(ident) match {

            /**
             * By default, load table operation is delegated to
             * [[org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog.loadTable(Identifier)]].
             * The method will wrap table metadata into [[V1Table]], then return.
             *
             * Here we need to unwrap it, then wrap catalogTable into SdbTableV2.
             */
            case v1: V1Table if SdbSourceUtils.isSdbTable(v1.catalogTable.provider) =>
                val catalogTable = v1.catalogTable

                logInfo(s"Loading sequoiadb data source V2 table, name: " +
                    s"${ident.namespace().mkString(".")}.${ident.name()}")

                val schema = if (catalogTable.schema.isEmpty) {
                    None
                } else {
                    Option(catalogTable.schema)
                }

                SdbTableV2(
                    spark,
                    Option(catalogTable),
                    Option(ident.name()),
                    schema)

            case other => other
        }
    }

    /**
     * Get provider of spark mapping table.
     * Actually, provider is the name of DataSource (like 'sequoiadb').
     *
     * @param properties
     * @return
     */
    def getProvider(properties: util.Map[String, String]): String = {
        Option(properties.get(TableCatalog.PROP_PROVIDER))
            .getOrElse(spark.sessionState.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
    }

    /**
     * The name of DataSource
     */
    override def shortName(): String = "sequoiadb"

    override def supportsExternalMetadata() = true

    override def inferSchema(options: CaseInsensitiveStringMap): StructType = null

    /**
     * Return a {@link SdbTableV2} instance with the specified table schema, partitioning (Not supported yet)
     * and properties to do read/write. The returned table should report the same schema and partitioning
     * with the specified ones, or Spark may fail the operation.
     *
     * @param schema       The specified table schema.
     * @param partitioning The specified table partitioning.
     * @param properties   The specified table properties.
     */
    override def getTable(
            schema: StructType,
            partitioning: Array[Transform],
            properties: util.Map[String, String])
    : Table = {
        val options = Map(properties.asScala.toSeq: _*)
        SdbTableV2(
            SparkSession.active,
            tableSchema = Option(schema),
            options = options)
    }

}
