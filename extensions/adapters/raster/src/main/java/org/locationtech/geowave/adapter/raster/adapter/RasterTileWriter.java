/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter;

import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class RasterTileWriter implements FieldWriter<RasterTile<?>> {

  @Override
  public byte[] writeField(final RasterTile<?> fieldValue) {
    // there is no need to preface the payload with the class name and a
    // length of the class name, the implementation is assumed to be known
    // on read so we can save space on persistence
    return fieldValue.toBinary();
  }
}
