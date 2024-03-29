/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.plugin;

import org.geotools.geometry.GeneralEnvelope;

/** This class allows us to make the GeoWaveRasterReader thread safe by storing its state here */
public class GeoWaveRasterReaderState {
  private final String coverageName;
  private boolean axisSwap = false;
  private GeneralEnvelope requestedEnvelope = null;
  private GeneralEnvelope requestEnvelopeXformed;

  public GeoWaveRasterReaderState(final String coverageName) {
    this.coverageName = coverageName;
  }

  /** @return the coverageName */
  public String getCoverageName() {
    return coverageName;
  }

  /** @return the boolean value of axisSwap */
  public boolean isAxisSwapped() {
    return axisSwap;
  }

  /** @param axisSwap the boolean value to set */
  public void setAxisSwap(final boolean axisSwap) {
    this.axisSwap = axisSwap;
  }

  /** @return the requestedEnvelope */
  public GeneralEnvelope getRequestedEnvelope() {
    return requestedEnvelope;
  }

  /** @param requestedEnvelope the requestedEnvelope to set */
  public void setRequestedEnvelope(final GeneralEnvelope requestedEnvelope) {
    this.requestedEnvelope = requestedEnvelope;
  }

  /** @return the requestEnvelopeXformed */
  public GeneralEnvelope getRequestEnvelopeXformed() {
    return requestEnvelopeXformed;
  }

  /** @param requestEnvelopeXformed the requestEnvelopeXformed to set */
  public void setRequestEnvelopeXformed(final GeneralEnvelope requestEnvelopeXformed) {
    this.requestEnvelopeXformed = requestEnvelopeXformed;
  }
}
