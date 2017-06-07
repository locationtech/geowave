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

public class ImageChipInfo
{
	private BufferedImage image;
	private byte[] imageBytes;
	private final int frameNumber;
	private final int pixelRow;
	private final int pixelColumn;

	public ImageChipInfo(
			final BufferedImage image,
			final int frameNumber,
			final int pixelRow,
			final int pixelColumn ) {
		this.image = image;
		this.frameNumber = frameNumber;
		this.pixelRow = pixelRow;
		this.pixelColumn = pixelColumn;
	}

	public BufferedImage getImage() {
		return image;
	}

	public void setImage(
			BufferedImage image ) {
		this.image = image;
	}

	public int getFrameNumber() {
		return frameNumber;
	}

	public int getPixelRow() {
		return pixelRow;
	}

	public int getPixelColumn() {
		return pixelColumn;
	}

	public byte[] getImageBytes() {
		return imageBytes;
	}

	public void setImageBytes(
			byte[] imageBytes ) {
		this.imageBytes = imageBytes;
	}

}
