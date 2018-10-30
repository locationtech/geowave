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
package org.locationtech.geowave.analytic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializationTool;

public class AdapterWithObjectWritable implements
		Writable
{
	private ObjectWritable objectWritable;
	private Short internalAdapterId = null;
	private ByteArray dataId;

	public void setObject(
			final ObjectWritable data ) {
		objectWritable = data;
	}

	public ObjectWritable getObjectWritable() {
		return objectWritable;
	}

	protected void setObjectWritable(
			final ObjectWritable objectWritable ) {
		this.objectWritable = objectWritable;
	}

	public Short getInternalAdapterId() {
		return internalAdapterId;
	}

	public void setInternalAdapterId(
			final short internalAdapterId ) {
		this.internalAdapterId = internalAdapterId;
	}

	public ByteArray getDataId() {
		return dataId;
	}

	public void setDataId(
			final ByteArray dataId ) {
		this.dataId = dataId;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		internalAdapterId = input.readShort();
		final int dataIdLength = input.readUnsignedShort();
		if (dataIdLength > 0) {
			final byte[] dataIdBinary = new byte[dataIdLength];
			input.readFully(dataIdBinary);
			dataId = new ByteArray(
					dataIdBinary);
		}

		if (objectWritable == null) {
			objectWritable = new ObjectWritable();
		}
		objectWritable.readFields(input);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		output.writeShort(internalAdapterId);
		if (dataId != null) {
			final byte[] dataIdBinary = dataId.getBytes();
			output.writeShort((short) dataIdBinary.length);
			output.write(dataIdBinary);
		}
		else {
			output.writeShort(0);
		}

		objectWritable.write(output);

	}

	public static void fillWritableWithAdapter(
			final HadoopWritableSerializationTool serializationTool,
			final AdapterWithObjectWritable writableToFill,
			final short internalAdapterId,
			final ByteArray dataId,
			final Object entry ) {
		writableToFill.setInternalAdapterId(internalAdapterId);
		writableToFill.setDataId(dataId);
		writableToFill.setObject(serializationTool.toWritable(
				internalAdapterId,
				entry));
	}

	public static Object fromWritableWithAdapter(
			final HadoopWritableSerializationTool serializationTool,
			final AdapterWithObjectWritable writableToExtract ) {
		final short internalAdapterId = writableToExtract.getInternalAdapterId();
		final Object innerObj = writableToExtract.objectWritable.get();
		return (innerObj instanceof Writable) ? serializationTool.getHadoopWritableSerializerForAdapter(
				internalAdapterId).fromWritable(
				(Writable) innerObj) : innerObj;
	}

	@Override
	public String toString() {
		return "AdapterWithObjectWritable [ internalAdapterId=" + internalAdapterId + ", dataId=" + dataId.getString()
				+ "]";
	}

}
