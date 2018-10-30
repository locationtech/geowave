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
package org.locationtech.geowave.analytic.kryo;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class PersistableSerializer extends
		Serializer<Persistable>
{

	@Override
	public Persistable read(
			Kryo kryo,
			Input input,
			Class<Persistable> classTag ) {

		// Read object byte count and allocate buffer to read object data
		int byteCount = input.readInt();
		byte[] bytes = new byte[byteCount];
		int bytesRead = input.read(bytes);
		// TODO: This was only added for findbugs warning, not really necessary
		// check
		if (bytesRead < 0) {
			return null;
		}

		return PersistenceUtils.fromBinary(bytes);
	}

	@Override
	public void write(
			Kryo kryo,
			Output output,
			Persistable object ) {

		// Persistence utils includes classId as short in front of persistable
		// object.
		byte[] serializedObj = PersistenceUtils.toBinary(object);
		int objLength = serializedObj.length;
		output.writeInt(objLength);
		output.write(serializedObj);
	}

}
