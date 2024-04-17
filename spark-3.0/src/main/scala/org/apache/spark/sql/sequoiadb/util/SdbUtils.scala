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

import com.sequoiadb.base.{CollectionSpace, DBCollection, Sequoiadb}
import com.sequoiadb.spark.{SdbConfig, SdbConnUtil, SdbException}
import org.apache.spark.SparkContext
import org.bson.util.JSON
import org.bson.{BSONObject, BasicBSONObject}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Sdb Connection Utils
 */
object SdbUtils {

    private val log_ = LoggerFactory.getLogger(SdbUtils.getClass)

    /**
     * Create a Sdb Connection by the given config
     */
    def create(sc: SparkContext, config: SdbConfig): Sequoiadb = {
        val sequoiadb = new Sequoiadb(
            config.host, config.username, config.password, SdbConfig.SdbConnectionOptions)

        // generate and set up source info (ignore failures)
        val sourceInfo = SdbConnUtil.generateSourceInfo(sc)
        SdbConnUtil
            .setupSourceSessionAttrIgnoreFailures(sequoiadb, sourceInfo)

        sequoiadb
    }

    /**
     * Get Collection Space
     *
     * @throws SdbException if collection space is not exist
     */
    def getCollectionSpace(sequoiadb: Sequoiadb, csName: String): CollectionSpace = {
        if (sequoiadb.isCollectionSpaceExist(csName)) {
            return sequoiadb.getCollectionSpace(csName)
        }
        throw new SdbException(s"Collection space $csName is not exist.")
    }

    /**
     * Get Collection
     *
     * @throws SdbException if collection is not exist
     */
    def getCollection(sequoiadb: Sequoiadb, csName: String, clName: String): DBCollection = {
        val collectionSpace = getCollectionSpace(sequoiadb, csName)
        if (collectionSpace.isCollectionExist(clName)) {
            return collectionSpace.getCollection(clName)
        }
        throw new SdbException(s"Collection $csName.$clName is not exist.")
    }

    /**
     * Ensure collection
     *  - Create collection space and collection if they are not existing.
     */
    def ensureCollection(sdb: Sequoiadb, csName: String, clName: String, config: Option[SdbConfig] = None): DBCollection = {
        if (!sdb.isCollectionSpaceExist(csName)) {
            val options = new BasicBSONObject()
            if (config.nonEmpty) {
                options.put("PageSize", config.get.pageSize)
                options.put("LobPageSize", config.get.lobPageSize)
                val domain = config.get.domain
                if (domain != "") {
                    options.put("Domain", domain)
                }
                log_.info(s"Using $options to create collection space[$csName]")
            }
            sdb.createCollectionSpace(csName, options)
        }
        val cs = sdb.getCollectionSpace(csName)
        if (!cs.isCollectionExist(clName)) {
            val options = new BasicBSONObject()
            if (config.nonEmpty) {
                if (config.get.shardingKey != "") {
                    val shardingKey = JSON.parse(config.get.shardingKey)
                        .asInstanceOf[BSONObject]
                    options.put("ShardingKey", shardingKey)
                    options.put("ShardingType", config.get.shardingType)
                    if (config.get.shardingType == SdbConfig.SHARDING_TYPE_HASH) {
                        options.put("Partition", config.get.clPartition)
                        if (config.get.autoSplit) {
                            options.put("AutoSplit", config.get.autoSplit)
                        }
                    }
                }
                options.put("ReplSize", config.get.replicaSize)
                if (config.get.compressionType != SdbConfig.COMPRESSION_TYPE_NONE) {
                    options.put("Compressed", true)
                    options.put("CompressionType", config.get.compressionType)
                }
                if (config.get.group != "") {
                    options.put("Group", config.get.group)
                }

                if (config.get.ensureShardingIndex != SdbConfig.DefaultEnsureShardingIndex) {
                    options.put("EnsureShardingIndex", config.get.ensureShardingIndex)
                }

                if (config.get.autoIndexId != SdbConfig.DefaultAutoIndexId) {
                    options.put("AutoIndexId", config.get.autoIndexId)
                }
                // auto increment key
                if (config.get.autoIncrement != "") {
                    val autoIncrement = JSON.parse(config.get.autoIncrement)
                        .asInstanceOf[BSONObject]
                    options.put("AutoIncrement", autoIncrement)
                }

                if (config.get.strictDataMode != SdbConfig.DefaultStrictDataMode) {
                    options.put("StrictDataMode", config.get.strictDataMode)
                }

                log_.info(s"Using $options to create collection[$csName.$clName]")
            }
            cs.createCollection(clName, options)
        }
        cs.getCollection(clName)
    }

}
