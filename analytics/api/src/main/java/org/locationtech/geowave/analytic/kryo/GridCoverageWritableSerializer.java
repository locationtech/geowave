/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.kryo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.locationtech.geowave.adapter.raster.adapter.GridCoverageWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class GridCoverageWritableSerializer extends Serializer<GridCoverageWritable> {
  static final Logger LOGGER = LoggerFactory.getLogger(FeatureSerializer.class);

  @Override
  public GridCoverageWritable read(
      final Kryo arg0,
      final Input arg1,
      final Class<GridCoverageWritable> arg2) {
    final GridCoverageWritable gcw = new GridCoverageWritable();
    final byte[] data = arg1.readBytes(arg1.readInt());
    try (DataInputStream is = new DataInputStream(new ByteArrayInputStream(data))) {
      gcw.readFields(is);
    } catch (final IOException e) {
      LOGGER.error("Cannot deserialize GridCoverageWritable", e);
      return null;
    }
    return gcw;
  }

  @Override
  public void write(final Kryo arg0, final Output arg1, final GridCoverageWritable arg2) {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream os = new DataOutputStream(bos)) {
      arg2.write(os);
      os.flush();
      final byte[] data = bos.toByteArray();
      arg1.writeInt(data.length);
      arg1.write(data);
    } catch (final IOException e) {
      LOGGER.error("Cannot serialize GridCoverageWritable", e);
    }
  }
}

