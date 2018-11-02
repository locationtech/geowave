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
package org.locationtech.geowave.core.geotime.store.field;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

import org.locationtech.jts.geom.Geometry;

public class GeometrySerializationProvider implements
		FieldSerializationProviderSpi<Geometry>
{
	@Override
	public FieldReader<Geometry> getFieldReader() {
		return new GeometryReader();
	}

	@Override
	public FieldWriter<Object, Geometry> getFieldWriter() {
		return new GeometryWriter();
	}

	protected static class GeometryReader implements
			FieldReader<Geometry>
	{
		@Override
		public Geometry readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return GeometryUtils.geometryFromBinary(fieldData);
		}
	}

	protected static class GeometryWriter implements
			FieldWriter<Object, Geometry>
	{
		@Override
		public byte[] writeField(
				final Geometry fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return GeometryUtils.geometryToBinary(fieldValue);
		}
	}

}
