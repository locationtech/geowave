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
package mil.nga.giat.geowave.adapter.vector.plugin;

/**
 * A basic, general exception thrown within the GeoWave plugin to GeoTools.
 * 
 */
public class GeoWavePluginException extends
		Exception
{

	private static final long serialVersionUID = -8043877412333078281L;

	public GeoWavePluginException(
			final String msg ) {
		super(
				msg);
	}

}
