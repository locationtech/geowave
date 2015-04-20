/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2008, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */

package mil.nga.giat.geowave.adapter.raster.plugin;

import org.geotools.geometry.GeneralEnvelope;

/**
 * Holds the state of the reader making it thread safe
 */

public class GeoWaveRasterReaderState
{
	private boolean xAxisSwitch = false;
	private final String coverageName;
	private GeneralEnvelope requestedEnvelope = null;
	private GeneralEnvelope requestEnvelopeTransformed = null;

	public GeoWaveRasterReaderState(
			final String coverageName ) {
		this.coverageName = coverageName;
	}

	public String getCoverageName() {
		return coverageName;
	}

	public GeneralEnvelope getRequestedEnvelope() {
		return requestedEnvelope;
	}

	public void setRequestedEnvelope(
			final GeneralEnvelope requestedEnvelope ) {
		this.requestedEnvelope = requestedEnvelope;
	}

	public GeneralEnvelope getRequestEnvelopeTransformed() {
		return requestEnvelopeTransformed;
	}

	public void setRequestEnvelopeTransformed(
			final GeneralEnvelope requestEnvelopeTransformed ) {
		this.requestEnvelopeTransformed = requestEnvelopeTransformed;
	}

	public boolean isXAxisSwitch() {
		return xAxisSwitch;
	}

	public void setXAxisSwitch(
			final boolean axisSwitch ) {
		xAxisSwitch = axisSwitch;
	}

}
