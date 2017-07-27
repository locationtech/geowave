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
package mil.nga.giat.geowave.analytic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializationTool;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

public class AdapterWithObjectWritable implements
		Writable
{
	private ObjectWritable objectWritable;
	private ByteArrayId adapterId;
	private ByteArrayId dataId;
	private boolean isPrimary;

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

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	public void setAdapterId(
			final ByteArrayId adapterId ) {
		this.adapterId = adapterId;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	public void setDataId(
			final ByteArrayId dataId ) {
		this.dataId = dataId;
	}

	public boolean isPrimary() {
		return isPrimary;
	}

	public void setPrimary(
			final boolean isPrimary ) {
		this.isPrimary = isPrimary;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		final int adapterIdLength = input.readInt();
		final byte[] adapterIdBinary = new byte[adapterIdLength];
		input.readFully(adapterIdBinary);
		adapterId = new ByteArrayId(
				adapterIdBinary);

		final int dataIdLength = input.readInt();
		if (dataIdLength > 0) {
			final byte[] dataIdBinary = new byte[dataIdLength];
			input.readFully(dataIdBinary);
			dataId = new ByteArrayId(
					dataIdBinary);
		}

		isPrimary = input.readBoolean();
		if (objectWritable == null) {
			objectWritable = new ObjectWritable();
		}
		objectWritable.readFields(input);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		final byte[] adapterIdBinary = adapterId.getBytes();
		output.writeInt(adapterIdBinary.length);
		output.write(adapterIdBinary);

		if (dataId != null) {
			final byte[] dataIdBinary = dataId.getBytes();
			output.writeInt(dataIdBinary.length);
			output.write(dataIdBinary);
		}
		else {
			output.writeInt(0);
		}

		output.writeBoolean(isPrimary);
		objectWritable.write(output);

	}

	public static void fillWritableWithAdapter(
			final HadoopWritableSerializationTool serializationTool,
			final AdapterWithObjectWritable writableToFill,
			final ByteArrayId adapterID,
			final ByteArrayId dataId,
			final boolean isPrimary,
			final Object entry ) {
		writableToFill.setAdapterId(adapterID);
		writableToFill.setPrimary(isPrimary);
		writableToFill.setDataId(dataId);
		writableToFill.setObject(serializationTool.toWritable(
				adapterID,
				entry));
	}

	public static Object fromWritableWithAdapter(
			final HadoopWritableSerializationTool serializationTool,
			final AdapterWithObjectWritable writableToExtract ) {
		final ByteArrayId adapterID = writableToExtract.getAdapterId();
		final Object innerObj = writableToExtract.objectWritable.get();
		return (innerObj instanceof Writable) ? serializationTool.getHadoopWritableSerializerForAdapter(
				adapterID).fromWritable(
				(Writable) innerObj) : innerObj;
	}

	@Override
	public String toString() {
		return "AdapterWithObjectWritable [ adapterId=" + adapterId + ", dataId=" + dataId + ", isPrimary=" + isPrimary
				+ "]";
	}

}
