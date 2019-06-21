/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.avro;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;

/**
 * This class can be sub-classed as a general-purpose recipe for parallelizing ingestion of files by
 * directly staging the binary of the file to Avro.
 */
public abstract class AbstractStageWholeFileToAvro<O> implements
    GeoWaveAvroFormatPlugin<AvroWholeFile, O> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStageWholeFileToAvro.class);

  @Override
  public Schema getAvroSchema() {
    return AvroWholeFile.getClassSchema();
  }

  @Override
  public CloseableIterator<AvroWholeFile> toAvroObjects(final URL f) {
    try {
      // TODO: consider a streaming mechanism in case a single file is too
      // large
      return new CloseableIterator.Wrapper<>(
          Iterators.singletonIterator(
              new AvroWholeFile(ByteBuffer.wrap(IOUtils.toByteArray(f)), f.getPath())));
    } catch (final IOException e) {
      LOGGER.warn("Unable to read file", e);
    }
    return new CloseableIterator.Empty<>();
  }
}
