/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.adapter.vector.ingest;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;

import com.beust.jcommander.Parameter;

public class DataSchemaOptionProvider implements
		Persistable,
		IngestFormatOptions
{
	@Parameter(names = "--extended", description = "A flag to indicate whether extended data format should be used")
	private boolean includeSupplementalFields = false;

	public boolean includeSupplementalFields() {
		return includeSupplementalFields;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			includeSupplementalFields ? (byte) 1 : (byte) 0
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes != null) && (bytes.length > 0)) {
			if (bytes[0] == 1) {
				includeSupplementalFields = true;
			}
		}
	}

	/**
	 *
	 */
	public void setSupplementalFields(
			final boolean supplementalFields ) {
		includeSupplementalFields = supplementalFields;
	}
}
