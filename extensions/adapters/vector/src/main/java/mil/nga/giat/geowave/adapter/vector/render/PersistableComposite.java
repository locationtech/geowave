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

import java.awt.AlphaComposite;
import java.awt.Composite;
import java.nio.ByteBuffer;

import org.geotools.renderer.composite.BlendComposite;
import org.geotools.renderer.composite.BlendComposite.BlendingMode;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public class PersistableComposite implements
		Persistable
{
	private boolean isBlend = true;
	private int blendModeOrAlphaRule = 0;
	private float alpha = 1f;

	public PersistableComposite() {}

	public PersistableComposite(
			final Composite composite ) {
		if (composite instanceof BlendComposite) {
			isBlend = true;
			blendModeOrAlphaRule = ((BlendComposite) composite).getBlend().ordinal();
			alpha = ((BlendComposite) composite).getAlpha();
		}
		else if (composite instanceof AlphaComposite) {
			isBlend = false;
			blendModeOrAlphaRule = ((AlphaComposite) composite).getRule();
			alpha = ((AlphaComposite) composite).getAlpha();
		}
	}

	public Composite getComposite() {
		if (isBlend) {
			return BlendComposite.getInstance(
					BlendingMode.values()[blendModeOrAlphaRule],
					alpha);
		}
		else {
			return AlphaComposite.getInstance(
					blendModeOrAlphaRule,
					alpha);
		}
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(9);
		buf.put(isBlend ? (byte) 0 : (byte) 1);
		buf.putInt(blendModeOrAlphaRule);
		buf.putFloat(alpha);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		isBlend = (buf.get() == 0);
		blendModeOrAlphaRule = buf.getInt();
		alpha = buf.getFloat();
	}
}
