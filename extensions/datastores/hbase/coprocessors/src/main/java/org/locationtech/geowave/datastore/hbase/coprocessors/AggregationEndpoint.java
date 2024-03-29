/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.datastore.hbase.coprocessors.protobuf.AggregationProtosServer;
import org.locationtech.geowave.datastore.hbase.filters.HBaseDistributableFilter;
import org.locationtech.geowave.datastore.hbase.filters.HBaseNumericIndexStrategyFilter;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class AggregationEndpoint extends AggregationProtosServer.AggregationService implements
    RegionCoprocessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationEndpoint.class);

  private RegionCoprocessorEnvironment env;

  @Override
  public Iterable<Service> getServices() {
    return Collections.singletonList(this);
  }

  @Override
  public void start(final CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(final CoprocessorEnvironment env) throws IOException {
    // nothing to do when coprocessor is shutting down
  }

  @Override
  public void aggregate(
      final RpcController controller,
      final AggregationProtosServer.AggregationRequest request,
      final RpcCallback<AggregationProtosServer.AggregationResponse> done) {
    FilterList filterList = null;
    InternalDataAdapter<?> dataAdapter = null;
    AdapterToIndexMapping indexMapping = null;
    Short internalAdapterId = null;
    AggregationProtosServer.AggregationResponse response = null;
    ByteString value = ByteString.EMPTY;

    // Get the aggregation type
    final Aggregation aggregation =
        (Aggregation) URLClassloaderUtils.fromClassId(
            request.getAggregation().getClassId().toByteArray());

    // Handle aggregation params
    if (request.getAggregation().hasParams()) {
      final byte[] parameterBytes = request.getAggregation().getParams().toByteArray();
      final Persistable aggregationParams = URLClassloaderUtils.fromBinary(parameterBytes);
      aggregation.setParameters(aggregationParams);
    }
    HBaseDistributableFilter hdFilter = null;
    if (aggregation != null) {

      if (request.hasRangeFilter()) {
        final byte[] rfilterBytes = request.getRangeFilter().toByteArray();

        try {
          final MultiRowRangeFilter rangeFilter = MultiRowRangeFilter.parseFrom(rfilterBytes);
          filterList = new FilterList(rangeFilter);
        } catch (final Exception e) {
          LOGGER.error("Error creating range filter.", e);
        }
      } else {
        LOGGER.error("Input range filter is undefined.");
      }
      if (request.hasNumericIndexStrategyFilter()) {
        final byte[] nisFilterBytes = request.getNumericIndexStrategyFilter().toByteArray();

        try {
          final HBaseNumericIndexStrategyFilter numericIndexStrategyFilter =
              HBaseNumericIndexStrategyFilter.parseFrom(nisFilterBytes);
          if (filterList == null) {
            filterList = new FilterList(numericIndexStrategyFilter);
          } else {
            filterList.addFilter(numericIndexStrategyFilter);
          }
        } catch (final Exception e) {
          LOGGER.error("Error creating index strategy filter.", e);
        }
      }

      try {
        // Add distributable filters if requested, this has to be last
        // in the filter list for the dedupe filter to work correctly
        if (request.hasModel()) {
          hdFilter = new HBaseDistributableFilter();

          if (request.hasWholeRowFilter()) {
            hdFilter.setWholeRowFilter(request.getWholeRowFilter());
          }

          if (request.hasPartitionKeyLength()) {
            hdFilter.setPartitionKeyLength(request.getPartitionKeyLength());
          }

          final byte[] filterBytes;
          if (request.hasFilter()) {
            filterBytes = request.getFilter().toByteArray();
          } else {
            filterBytes = null;
          }
          final byte[] modelBytes = request.getModel().toByteArray();

          if (hdFilter.init(filterBytes, modelBytes)) {
            if (filterList == null) {
              filterList = new FilterList(hdFilter);
            } else {
              filterList.addFilter(hdFilter);
            }
          } else {
            LOGGER.error("Error creating distributable filter.");
          }
        } else {
          LOGGER.error("Input distributable filter is undefined.");
        }
      } catch (final Exception e) {
        LOGGER.error("Error creating distributable filter.", e);
      }

      if (request.hasAdapter()) {
        final byte[] adapterBytes = request.getAdapter().toByteArray();
        dataAdapter = (InternalDataAdapter<?>) URLClassloaderUtils.fromBinary(adapterBytes);
      }
      if (request.hasInternalAdapterId()) {
        final byte[] adapterIdBytes = request.getInternalAdapterId().toByteArray();
        internalAdapterId = ByteArrayUtils.byteArrayToShort(adapterIdBytes);
      }
      if (request.hasIndexMapping()) {
        final byte[] mappingBytes = request.getIndexMapping().toByteArray();
        indexMapping = (AdapterToIndexMapping) URLClassloaderUtils.fromBinary(mappingBytes);
      }
      final String[] authorizations;
      if (request.hasVisLabels()) {
        final byte[] visBytes = request.getVisLabels().toByteArray();
        if (visBytes.length > 0) {
          authorizations = StringUtils.stringsFromBinary(visBytes);
        } else {
          authorizations = null;
        }
      } else {
        authorizations = null;
      }

      try {
        final Object result =
            getValue(
                aggregation,
                filterList,
                dataAdapter,
                indexMapping,
                internalAdapterId,
                hdFilter,
                request.getBlockCaching(),
                request.getCacheSize(),
                authorizations);

        URLClassloaderUtils.initClassLoader();
        final byte[] bvalue = aggregation.resultToBinary(result);
        value = ByteString.copyFrom(bvalue);
      } catch (final IOException ioe) {
        LOGGER.error("Error during aggregation.", ioe);

        /*
         * ResponseConverter.setControllerException( controller, ioe);
         */
      } catch (final Exception e) {
        LOGGER.error("Error during aggregation.", e);
      }
    }

    response = AggregationProtosServer.AggregationResponse.newBuilder().setValue(value).build();

    done.run(response);
  }

  private Object getValue(
      final Aggregation aggregation,
      final Filter filter,
      final InternalDataAdapter<?> dataAdapter,
      final AdapterToIndexMapping indexMapping,
      final Short internalAdapterId,
      final HBaseDistributableFilter hdFilter,
      final boolean blockCaching,
      final int scanCacheSize,
      final String[] authorizations) throws IOException {
    final Scan scan = new Scan();
    scan.setMaxVersions(1);
    scan.setCacheBlocks(blockCaching);

    if (scanCacheSize != HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING) {
      scan.setCaching(scanCacheSize);
    }

    if (filter != null) {
      scan.setFilter(filter);
    }

    if (internalAdapterId != null) {
      scan.addFamily(StringUtils.stringToBinary(ByteArrayUtils.shortToString(internalAdapterId)));
    }

    if (authorizations != null) {
      scan.setAuthorizations(new Authorizations(authorizations));
    }
    ((HRegion) env.getRegion()).getCoprocessorHost().preScannerOpen(scan);
    try (InternalScanner scanner = env.getRegion().getScanner(scan)) {
      final List<Cell> results = new ArrayList<>();
      boolean hasNext;
      do {
        hasNext = scanner.next(results);
        if (!results.isEmpty()) {
          if (hdFilter != null) {
            if (dataAdapter != null) {
              final Object row = hdFilter.decodeRow(dataAdapter, indexMapping);

              if (row != null) {
                aggregation.aggregate(dataAdapter, row);
              } else {
                LOGGER.error("DataAdapter failed to decode row");
              }
            } else {
              aggregation.aggregate(null, hdFilter.getPersistenceEncoding());
            }
          } else {
            aggregation.aggregate(dataAdapter, null);
          }
          results.clear();
        }
      } while (hasNext);
    }
    return aggregation.getResult();
  }
}
