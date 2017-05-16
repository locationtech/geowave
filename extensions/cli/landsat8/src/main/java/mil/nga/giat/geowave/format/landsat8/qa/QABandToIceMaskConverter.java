/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.landsat8.qa;

import java.awt.image.DataBuffer;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;

import mil.nga.giat.geowave.format.landsat8.BandFeatureIterator;
import mil.nga.giat.geowave.format.landsat8.Landsat8BandConverterSpi;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.opengis.feature.simple.SimpleFeature;

public class QABandToIceMaskConverter implements
		Landsat8BandConverterSpi
{
	private static final int doubleBitMask = 0x0003;
	private static final int tripleBitMask = 0x0007;

	@Override
	public GridCoverage2D convert(
			final String coverageName,
			final GridCoverage2D originalBandData,
			final SimpleFeature bandMetadata ) {
		final Object attrValue = bandMetadata.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME);
		if ("BQA".equalsIgnoreCase(attrValue.toString())) {
			final MultiPixelPackedSampleModel newSampleModel = new MultiPixelPackedSampleModel(
					DataBuffer.TYPE_BYTE,
					originalBandData.getRenderedImage().getWidth(),
					originalBandData.getRenderedImage().getHeight(),
					2);
			final WritableRaster nextRaster = Raster.createWritableRaster(
					newSampleModel,
					null);
			final RenderedImage image = originalBandData.getRenderedImage();
			final Raster data = image.getData();
			for (int x = 0; x < data.getWidth(); x++) {
				for (int y = 0; y < data.getHeight(); y++) {
					final int sample = getIceSample(
							x,
							y,
							data);
					nextRaster.setSample(
							x,
							y,
							0,
							sample);
				}
			}
			final GridCoverage2D nextCov = new GridCoverageFactory().create(
					coverageName,
					nextRaster,
					originalBandData.getEnvelope());
			return nextCov;
		}
		return originalBandData;
	}

	/**
	 * returns -1 if the sample is not valid, returns 0 if the sample is no ice,
	 * and return 1 if the sample is ice
	 * 
	 * @return
	 */
	private int getIceSample(
			final int x,
			final int y,
			final Raster data ) {
		// if (x < 0 || y < 0 || x >= data.getWidth() || y >= data.getHeight())
		// {
		// return -1;
		// }
		final int sample = data.getSample(
				x,
				y,
				0);
		if ((sample & tripleBitMask) > 0) {
			return 0x00;
		}
		else if ((((sample >> 14) & doubleBitMask) == 3) || (((sample >> 12) & doubleBitMask) == 3)) {
			return 0x01;
		}
		return (((sample >> 10) & doubleBitMask) > 1) ? 0x03 : 0x02;
	}

	@Override
	public String getName() {
		return "icemask";
	}

}
