/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

/**
 * This is a persistable version of the IndexFieldHandler so that customized field handlers can be
 * automatically persisted with the data adapter. By default the field handlers assume that they can
 * be recreated without custom serialization necessary but if it is necessary, the field handler
 * should implement this interface.
 *
 * @param <RowType>
 * @param <IndexFieldType>
 * @param <NativeFieldType>
 */
public interface PersistentIndexFieldHandler<RowType, IndexFieldType extends CommonIndexValue, NativeFieldType>
    extends
    IndexFieldHandler<RowType, IndexFieldType, NativeFieldType>,
    Persistable {
}
