/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.persist;

/**
 * Marker interface for internal GeoWave persistable registries. Third-party additions to GeoWave
 * should NOT use this interface. Any persistable registry that does not implement this interface
 * will be automatically converted to the negative persistable ID space. This allows third-parties
 * to be able to use the full range of positive persistable IDs without worrying about colliding
 * with a pre-existing internal persistable ID.
 */
public interface InternalPersistableRegistry {

}
