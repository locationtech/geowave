/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.persist;

import java.util.function.Supplier;

/**
 * Registers new persistables with GeoWave. Each persistable has an ID of type short that uniquely
 * identifies the class. Internal GeoWave persistable registries also implement the
 * {@link InternalPersistableRegistry} marker interface that alleviates potential ID conflicts with
 * third-party plugins. Any third-party persistable that does not implement the internal marker
 * interface will automatically be converted to the negative ID space (i.e. a persistable ID of 30
 * will become -30). This allows third-party developers to use any persistable ID without having to
 * worry about conflicting with current or future internal persistables.
 */
public interface PersistableRegistrySpi {

  public PersistableIdAndConstructor[] getSupportedPersistables();

  public static class PersistableIdAndConstructor {
    private final short persistableId;
    private final Supplier<Persistable> persistableConstructor;

    public PersistableIdAndConstructor(
        final short persistableId,
        final Supplier<Persistable> persistableConstructor) {
      this.persistableId = persistableId;
      this.persistableConstructor = persistableConstructor;
    }

    public short getPersistableId() {
      return persistableId;
    }

    public Supplier<Persistable> getPersistableConstructor() {
      return persistableConstructor;
    }
  }
}
