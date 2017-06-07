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

import java.awt.Graphics;
import java.awt.Image;
import java.awt.image.BufferedImage;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;

public class ImageChipUtils
{
	public static BufferedImage getImage(
			final Image img,
			final int targetPixelSize,
			final int type ) {
		final int currentWidth = img.getWidth(null);
		final int currentHeight = img.getHeight(null);
		final int currentPixelSize = Math.max(
				currentWidth,
				currentHeight);
		final double scaleFactor = (double) targetPixelSize / (double) currentPixelSize;
		return getScaledImageOfType(
				img,
				(int) (currentWidth * scaleFactor),
				(int) (currentHeight * scaleFactor),
				type);
	}

	public static BufferedImage getScaledImageOfType(
			final Image img,
			final int width,
			final int height,
			final int type ) {
		if (img instanceof BufferedImage) {
			if ((((BufferedImage) img).getType() == type) && (img.getWidth(null) == width)
					&& (img.getHeight(null) == height)) {
				return (BufferedImage) img;
			}
		}
		final BufferedImage scaledImage = toBufferedImage(
				img.getScaledInstance(
						width,
						height,
						Image.SCALE_SMOOTH),
				type);
		return scaledImage;
	}

	public static BufferedImage toBufferedImage(
			final Image image,
			final int type ) {
		final BufferedImage bi = new BufferedImage(
				image.getWidth(null),
				image.getHeight(null),
				type);
		final Graphics g = bi.getGraphics();
		g.drawImage(
				image,
				0,
				0,
				null);
		g.dispose();
		return bi;
	}

	public static ByteArrayId getDataId(
			final String mission,
			final String trackId,
			final long timeMillis ) {
		return new ByteArrayId(
				mission + "/" + trackId + "/" + timeMillis);
	}

	public static ByteArrayId getTrackDataIdPrefix(
			final String mission,
			final String trackId ) {
		return new ByteArrayId(
				mission + "/" + trackId + "/");
	}

	public static ImageChip fromDataIdAndValue(
			final ByteArrayId dataId,
			final byte[] value ) {
		final String dataIdStr = StringUtils.stringFromBinary(dataId.getBytes());
		final String[] split = dataIdStr.split("/");
		return new ImageChip(
				split[0],
				split[1],
				Long.parseLong(split[2]),
				value);
	}
}
