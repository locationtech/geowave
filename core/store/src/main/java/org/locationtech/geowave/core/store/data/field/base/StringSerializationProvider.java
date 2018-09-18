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
package org.locationtech.geowave.core.store.data.field.base;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class StringSerializationProvider implements
		FieldSerializationProviderSpi<String>
{

	@Override
	public FieldReader<String> getFieldReader() {
		return new StringReader();
	}

	@Override
	public FieldWriter<Object, String> getFieldWriter() {
		return new StringWriter();
	}

	protected static class StringReader implements
			FieldReader<String>
	{

		@Override
		public String readField(
				final byte[] fieldData ) {
			if ((fieldData == null) || (fieldData.length < 1)) {
				return null;
			}
			return StringUtils.stringFromBinary(fieldData);

			// for field serialization ensure UTF-8?
			// return new String(
			// fieldData,
			// StringUtils.UTF8_CHAR_SET);
		}
	}

	protected static class StringWriter implements
			FieldWriter<Object, String>
	{
		@Override
		public byte[] writeField(
				final String fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return StringUtils.stringToBinary(fieldValue);

			// for field serialization ensure UTF-8?
			// return fieldValue.getBytes(StringUtils.UTF8_CHAR_SET);
		}
	}
}
