/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.iterators;

import java.io.IOException;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExceptionHandlingSkippingIterator extends SkippingIterator {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExceptionHandlingSkippingIterator.class);

  @Override
  protected final void consume() throws IOException {
    try {
      consumeInternal();
    } catch (final IOException e) {
      throw e;
    } catch (final Exception e) {
      LOGGER.error("Exception while initializing skipping iterator", e);
      throw new IOException(e);
    }
  }

  protected abstract void consumeInternal() throws IOException;

}
