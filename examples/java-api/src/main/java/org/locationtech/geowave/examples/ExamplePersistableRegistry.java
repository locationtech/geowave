/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples;

import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.examples.adapter.CustomAdapterExample.POIBasicDataAdapter;
import org.locationtech.geowave.examples.index.CustomIndexExample.UUIDConstraints;
import org.locationtech.geowave.examples.index.CustomIndexExample.UUIDIndexStrategy;
import org.locationtech.geowave.examples.ingest.plugin.CustomIngestPlugin;

public class ExamplePersistableRegistry implements PersistableRegistrySpi {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 20000, POIBasicDataAdapter::new),
        new PersistableIdAndConstructor((short) 20001, UUIDIndexStrategy::new),
        new PersistableIdAndConstructor((short) 20002, UUIDConstraints::new),
        new PersistableIdAndConstructor((short) 20003, CustomIngestPlugin::new)};
  }
}
