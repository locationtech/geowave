/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import org.geotools.process.factory.AnnotatedBeanProcessFactory;
import org.geotools.text.Text;

/**
 * This is the GeoTools Factory for introducing the nga:Decimation rendering transform. GeoTools
 * uses Java SPI to inject the WPS process (see
 * META-INF/services/org.geotools.process.ProcessFactory).
 */
public class GeoWaveGSProcessFactory extends AnnotatedBeanProcessFactory {

  public GeoWaveGSProcessFactory() {
    super(
        Text.text("GeoWave Process Factory"),
        "geowave",
        SubsampleProcess.class,
        DistributedRenderProcess.class);
  }
}
