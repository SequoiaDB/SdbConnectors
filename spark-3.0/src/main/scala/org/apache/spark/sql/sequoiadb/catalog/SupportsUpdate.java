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

package org.apache.spark.sql.sequoiadb.catalog;

import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.sources.Filter;

import java.util.List;

/**
 * A custom mix-in interface for {@link org.apache.spark.sql.connector.catalog.Table}
 * update support.
 * Data Source V2 Table can implement this interface to provide the ability to update
 * records in tables that matches filter expressions.
 */
public interface SupportsUpdate {

    /**
     * Checks if it is possible to update records in a data source V2 table
     * that matches filter expressions.
     * Records should be updated iff all the filter expressions matches. That
     * is, the expressions must be interpreted as a set of filters that are
     * ANDed together.
     *
     * @param filters
     * @return
     */
    boolean canUpdateWhere(Filter[] filters);

    /**
     * Performs update on an external DataSource by filters and assignments
     *
     * @param filters
     * @param assignments
     */
    void updateWhere(Filter[] filters, List<Assignment> assignments);

}
