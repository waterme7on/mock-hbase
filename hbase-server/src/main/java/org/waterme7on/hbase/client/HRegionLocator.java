/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.waterme7on.hbase.client;

import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.REGION_NAMES_KEY;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.trace.TraceUtil;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * An implementation of {@link RegionLocator}. Used to view region location information for a single
 * HBase table. Lightweight. Get as needed and just close when done. Instances of this class SHOULD
 * NOT be constructed directly. Obtain an instance via {@link Connection}. See
 * {@link ConnectionFactory} class comment for an example of how.
 * <p/>
 * This class is thread safe
 */
public class HRegionLocator {

    private final TableName tableName;
    private final ConnectionImplementation connection;

    public HRegionLocator(TableName tableName, ConnectionImplementation connection) {
        this.connection = connection;
        this.tableName = tableName;
    }



    private <R, T extends Throwable> R tracedLocationFuture(TraceUtil.ThrowingCallable<R, T> action,
                                                            Function<R, List<String>> getRegionNames, Supplier<Span> spanSupplier) throws T {
        final Span span = spanSupplier.get();
        try (Scope ignored = span.makeCurrent()) {
            final R result = action.call();
            final List<String> regionNames = getRegionNames.apply(result);
            if (!CollectionUtils.isEmpty(regionNames)) {
                span.setAttribute(REGION_NAMES_KEY, regionNames);
            }
            span.setStatus(StatusCode.OK);
            span.end();
            return result;
        } catch (Throwable e) {
            TraceUtil.setError(span, e);
            span.end();
            throw e;
        }
    }
}
