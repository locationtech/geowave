/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.minicluster;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;

public class GeoWaveMiniAccumuloClusterImpl extends MiniAccumuloClusterImpl {

  public GeoWaveMiniAccumuloClusterImpl(final MiniAccumuloConfigImpl config) throws IOException {
    super(config);
  }

  public void setExternalShutdownExecutor(final ExecutorService svc) {
    setShutdownExecutor(svc);
  }
}
