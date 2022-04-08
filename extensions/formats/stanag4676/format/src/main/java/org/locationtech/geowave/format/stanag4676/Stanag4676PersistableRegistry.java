/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676;

import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.format.stanag4676.Stanag4676IngestPlugin.IngestWithReducerImpl;
import org.locationtech.geowave.format.stanag4676.image.ImageChipDataAdapter;

public class Stanag4676PersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 800, IngestWithReducerImpl::new),
        new PersistableIdAndConstructor((short) 801, ImageChipDataAdapter::new),};
  }
}
