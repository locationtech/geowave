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
package mil.nga.giat.geowave.format.stanag4676.image;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageChip
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ImageChip.class);
	private final String mission;
	private final String trackId;
	private final long timeMillis;
	private final byte[] imageBinary;
	private BufferedImage image;

	public ImageChip(
			final String mission,
			final String trackId,
			final long timeMillis,
			final byte[] imageBinary ) {
		this.mission = mission;
		this.trackId = trackId;
		this.timeMillis = timeMillis;
		this.imageBinary = imageBinary;
	}

	public String getMission() {
		return mission;
	}

	public String getTrackId() {
		return trackId;
	}

	public long getTimeMillis() {
		return timeMillis;
	}

	public byte[] getImageBinary() {
		return imageBinary;
	}

	public ByteArrayId getDataId() {
		return ImageChipUtils.getDataId(
				mission,
				trackId,
				timeMillis);
	}

	public BufferedImage getImage(
			final int targetPixelSize ) {
		if (targetPixelSize <= 0) {
			final BufferedImage img = getImage();
			if ((img != null) && (img.getType() != BufferedImage.TYPE_3BYTE_BGR)) {
				return ImageChipUtils.toBufferedImage(
						img,
						BufferedImage.TYPE_3BYTE_BGR);
			}
		}
		return ImageChipUtils.getImage(
				getImage(),
				targetPixelSize,
				BufferedImage.TYPE_3BYTE_BGR);
	}

	private synchronized BufferedImage getImage() {
		if (image == null) {
			try {
				image = ImageIO.read(new ByteArrayInputStream(
						imageBinary));
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to read image chip",
						e);
			}
		}
		return image;
	}
}
