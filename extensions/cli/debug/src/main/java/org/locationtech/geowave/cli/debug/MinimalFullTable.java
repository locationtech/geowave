/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.debug;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.time.StopWatch;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloRequiredOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.locationtech.geowave.datastore.hbase.HBaseStoreFactoryFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

@GeowaveOperation(name = "fullscanMinimal", parentOperation = DebugSection.class)
@Parameters(commandDescription = "full table scan without any iterators or deserialization")
public class MinimalFullTable extends DefaultOperation implements Command {
  private static Logger LOGGER = LoggerFactory.getLogger(MinimalFullTable.class);

  @Parameter(description = "<storename>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(names = "--indexId", required = true, description = "The name of the index (optional)")
  private String indexId;

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public void execute(final OperationParams params) throws ParseException {
    final StopWatch stopWatch = new StopWatch();

    // Ensure we have all the required arguments
    if (parameters.size() != 1) {
      throw new ParameterException("Requires arguments: <storename>");
    }

    final String storeName = parameters.get(0);

    // Attempt to load store.
    final DataStorePluginOptions storeOptions =
        CLIUtils.loadStore(storeName, getGeoWaveConfigFile(params), params.getConsole());

    final String storeType = storeOptions.getType();

    if (storeType.equals(AccumuloStoreFactoryFamily.TYPE)) {
      try {
        final AccumuloRequiredOptions opts =
            (AccumuloRequiredOptions) storeOptions.getFactoryOptions();

        final AccumuloOperations ops =
            new AccumuloOperations(
                opts.getZookeeper(),
                opts.getInstance(),
                opts.getUser(),
                opts.getPasswordOrKeytab(),
                opts.isUseSasl(),
                opts.getGeoWaveNamespace(),
                (AccumuloOptions) opts.getStoreOptions());

        long results = 0;
        final BatchScanner scanner = ops.createBatchScanner(indexId);
        scanner.setRanges(Collections.singleton(new Range()));
        final Iterator<Entry<Key, Value>> it = scanner.iterator();

        stopWatch.start();
        while (it.hasNext()) {
          it.next();
          results++;
        }
        stopWatch.stop();

        scanner.close();
        System.out.println("Got " + results + " results in " + stopWatch.toString());
      } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException
          | IOException e) {
        LOGGER.error("Unable to scan accumulo datastore", e);
      }
    } else if (storeType.equals(HBaseStoreFactoryFamily.TYPE)) {
      throw new UnsupportedOperationException(
          "full scan for store type " + storeType + " not yet implemented.");
    } else {
      throw new UnsupportedOperationException(
          "full scan for store type " + storeType + " not implemented.");
    }
  }
}
