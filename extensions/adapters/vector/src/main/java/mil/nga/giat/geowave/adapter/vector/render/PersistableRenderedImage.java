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
package mil.nga.giat.geowave.adapter.vector.render;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ar.com.hjg.pngj.FilterType;
import it.geosolutions.imageio.plugins.png.PNGWriter;
import mil.nga.giat.geowave.core.index.persist.Persistable;

/**
 * This class wraps a rendered image as a GeoWave Persistable object. It
 * serializes and deserializes the BufferedImage as a png using ImageIO.
 *
 */
public class PersistableRenderedImage implements
		Persistable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(PersistableRenderedImage.class);
	private final static float DEFAULT_PNG_QUALITY = 0.8f;
	public BufferedImage image;

	public PersistableRenderedImage() {}

	public PersistableRenderedImage(
			final BufferedImage image ) {
		this.image = image;
	}

	public BufferedImage getImage() {
		return image;
	}

	@Override
	public byte[] toBinary() {
		if (image == null) {
			return new byte[0];
		}
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			// we could just use the expected output format, but that may not be
			// correct, instead we use PNG

			// it seems that even though the requested image may be jpeg
			// example, the individual styles may need to retain transparency to
			// be composited correctly
			final PNGWriter writer = new PNGWriter();
			image = (BufferedImage) writer.writePNG(
					image,
					baos,
					DEFAULT_PNG_QUALITY,
					FilterType.FILTER_NONE);
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to serialize image",
					e);
		}
		return baos.toByteArray();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length == 0) {
			return;
		}
		final ByteArrayInputStream bais = new ByteArrayInputStream(
				bytes);
		try {
			image = ImageIO.read(bais);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to deserialize image",
					e);
		}
	}
}
