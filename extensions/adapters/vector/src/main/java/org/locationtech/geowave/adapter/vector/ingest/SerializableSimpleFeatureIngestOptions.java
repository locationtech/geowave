/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.ingest;

import com.beust.jcommander.ParametersDelegate;

/**
 * An extension of simple feature ingest options that provides additional serialization options to
 * be specified.
 */
public class SerializableSimpleFeatureIngestOptions extends SimpleFeatureIngestOptions {

  @ParametersDelegate
  private FeatureSerializationOptionProvider serializationFormatOptionProvider =
      new FeatureSerializationOptionProvider();

  public FeatureSerializationOptionProvider getSerializationFormatOptionProvider() {
    return serializationFormatOptionProvider;
  }

  public void setSerializationFormatOptionProvider(
      final FeatureSerializationOptionProvider serializationFormatOptionProvider) {
    this.serializationFormatOptionProvider = serializationFormatOptionProvider;
  }

}
