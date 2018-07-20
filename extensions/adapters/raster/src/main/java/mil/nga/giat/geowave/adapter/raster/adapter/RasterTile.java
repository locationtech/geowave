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
package mil.nga.giat.geowave.adapter.raster.adapter;

import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferDouble;
import java.awt.image.DataBufferFloat;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

import me.lemire.integercompression.differential.IntegratedIntCompressor;
import mil.nga.giat.geowave.adapter.raster.protobuf.DataBufferProtos;
import mil.nga.giat.geowave.adapter.raster.protobuf.DataBufferProtos.ByteDataBuffer;
import mil.nga.giat.geowave.adapter.raster.protobuf.DataBufferProtos.DoubleArray;
import mil.nga.giat.geowave.adapter.raster.protobuf.DataBufferProtos.DoubleDataBuffer;
import mil.nga.giat.geowave.adapter.raster.protobuf.DataBufferProtos.FloatArray;
import mil.nga.giat.geowave.adapter.raster.protobuf.DataBufferProtos.FloatDataBuffer;
import mil.nga.giat.geowave.adapter.raster.protobuf.DataBufferProtos.SignedIntArray;
import mil.nga.giat.geowave.adapter.raster.protobuf.DataBufferProtos.SignedIntDataBuffer;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;

public class RasterTile<T extends Persistable> implements
		Mergeable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RasterTile.class);
	private DataBuffer dataBuffer;
	private T metadata;

	public RasterTile() {
		super();
	}

	public RasterTile(
			final DataBuffer dataBuffer,
			final T metadata ) {
		this.dataBuffer = dataBuffer;
		this.metadata = metadata;
	}

	public DataBuffer getDataBuffer() {
		return dataBuffer;
	}

	public T getMetadata() {
		return metadata;
	}

	protected static byte[] getDataBufferBinary(
			final DataBuffer dataBuffer ) {
		final DataBufferProtos.DataBuffer.Builder bldr = DataBufferProtos.DataBuffer.newBuilder();
		bldr
				.setType(
						dataBuffer.getDataType());
		bldr
				.addAllOffsets(
						Ints
								.asList(
										dataBuffer.getOffsets()));
		bldr
				.setSize(
						dataBuffer.getSize());
		switch (dataBuffer.getDataType()) {
			case DataBuffer.TYPE_BYTE:
				final ByteDataBuffer.Builder byteBldr = ByteDataBuffer.newBuilder();
				final byte[][] byteBank = ((DataBufferByte) dataBuffer).getBankData();
				final Iterable<ByteString> byteIt = () -> new Iterator<ByteString>() {
					private int index = 0;

					@Override
					public boolean hasNext() {
						return byteBank.length > index;
					}

					@Override
					public ByteString next() {
						if (!hasNext()) {
							throw new NoSuchElementException();
						}
						return ByteString
								.copyFrom(
										byteBank[index++]);
					}

				};
				byteBldr
						.addAllBanks(
								byteIt);
				bldr
						.setByteDb(
								byteBldr.build());
				break;
			case DataBuffer.TYPE_SHORT:
				setBuilder(
						shortToInt(
								((DataBufferUShort) dataBuffer).getBankData()),
						bldr);
				break;
			case DataBuffer.TYPE_USHORT:
				setBuilder(
						shortToInt(
								((DataBufferUShort) dataBuffer).getBankData()),
						bldr);
				break;
			case DataBuffer.TYPE_INT:
				setBuilder(
						((DataBufferInt) dataBuffer).getBankData(),
						bldr);
				break;
			case DataBuffer.TYPE_FLOAT:
				final FloatDataBuffer.Builder fltBldr = FloatDataBuffer.newBuilder();
				final float[][] fltBank = ((DataBufferFloat) dataBuffer).getBankData();
				final Iterable<FloatArray> floatIt = () -> new Iterator<FloatArray>() {
					private int index = 0;

					@Override
					public boolean hasNext() {
						return fltBank.length > index;
					}

					@Override
					public FloatArray next() {
						return FloatArray
								.newBuilder()
								.addAllSamples(
										Floats
												.asList(
														fltBank[index++]))
								.build();
					}

				};
				fltBldr
						.addAllBanks(
								floatIt);
				bldr
						.setFlt(
								fltBldr);
				break;
			case DataBuffer.TYPE_DOUBLE:
				final DoubleDataBuffer.Builder dblBldr = DoubleDataBuffer.newBuilder();
				final double[][] dblBank = ((DataBufferDouble) dataBuffer).getBankData();
				final Iterable<DoubleArray> dblIt = () -> new Iterator<DoubleArray>() {
					private int index = 0;

					@Override
					public boolean hasNext() {
						return dblBank.length > index;
					}

					@Override
					public DoubleArray next() {
						return DoubleArray
								.newBuilder()
								.addAllSamples(
										Doubles
												.asList(
														dblBank[index++]))
								.build();
					}

				};
				dblBldr
						.addAllBanks(
								dblIt);
				bldr
						.setDbl(
								dblBldr);
				break;
			default:
				throw new RuntimeException();
		}
		return bldr.build().toByteArray();
	}

	private static void setBuilder(
			int[][] intBank,
			DataBufferProtos.DataBuffer.Builder bldr ) {
		IntegratedIntCompressor iic = new IntegratedIntCompressor();
		final SignedIntDataBuffer.Builder intBldr = SignedIntDataBuffer.newBuilder();
		final Iterable<SignedIntArray> intIt = () -> new Iterator<SignedIntArray>() {
			private int index = 0;

			@Override
			public boolean hasNext() {
				return intBank.length > index;
			}

			@Override
			public SignedIntArray next() {
				final int[] internalArray = intBank[index++];
				int[] compressed = iic
						.compress(
								internalArray);
				return SignedIntArray
						.newBuilder()
						.addAllSamples(
								Ints
										.asList(
												compressed))
						.build();
			}

		};
		intBldr
				.addAllBanks(
						intIt);
		bldr
				.setSint(
						intBldr);
	}

	protected static DataBuffer getDataBuffer(
			final byte[] binary )
			throws IOException,
			ClassNotFoundException {
		// // Read serialized form from the stream.
		DataBufferProtos.DataBuffer buffer = DataBufferProtos.DataBuffer.parseFrom(binary);

		int[] offsets = ArrayUtils.toPrimitive(buffer.getOffsetsList().toArray(
				new Integer[] {}));
		// Restore the transient DataBuffer.
		switch (buffer.getType()) {
			case DataBuffer.TYPE_BYTE:
				return new DataBufferByte(
						(byte[][]) listToByte(buffer.getByteDb().getBanksList()),
						buffer.getSize(),
						offsets);
			case DataBuffer.TYPE_SHORT:
				return new DataBufferShort(
						intToShort(listToInt(buffer.getSint().getBanksList())),
						buffer.getSize(),
						offsets);
			case DataBuffer.TYPE_USHORT:
				return new DataBufferUShort(
						intToShort(listToInt(buffer.getSint().getBanksList())),
						buffer.getSize(),
						offsets);
			case DataBuffer.TYPE_INT:
				return new DataBufferInt(
						listToInt(buffer.getSint().getBanksList()),
						buffer.getSize(),
						offsets);
			case DataBuffer.TYPE_FLOAT:
				return new DataBufferFloat(
						listToFloat(buffer.getFlt().getBanksList()),
						buffer.getSize(),
						offsets);
			case DataBuffer.TYPE_DOUBLE:
				return new DataBufferDouble(
						listToDouble(buffer.getDbl().getBanksList()),
						buffer.getSize(),
						offsets);
			default:
				throw new RuntimeException();
		}
	}

	private static byte[][] listToByte(
			List<ByteString> list ) {
		byte[][] retVal = new byte[list.size()][];
		for (int i = 0; i < list.size(); i++) {
			retVal[i] = list.get(
					i).toByteArray();
		}
		return retVal;

	}

	private static float[][] listToFloat(
			List<FloatArray> list ) {
		float[][] retVal = new float[list.size()][];
		for (int i = 0; i < list.size(); i++) {
			List<Float> internalList = list.get(
					i).getSamplesList();
			retVal[i] = ArrayUtils.toPrimitive(internalList.toArray(new Float[internalList.size()]));
		}
		return retVal;
	}

	private static double[][] listToDouble(
			List<DoubleArray> list ) {
		double[][] retVal = new double[list.size()][];
		for (int i = 0; i < list.size(); i++) {
			List<Double> internalList = list.get(
					i).getSamplesList();
			retVal[i] = ArrayUtils.toPrimitive(internalList.toArray(new Double[internalList.size()]));
		}
		return retVal;
	}

	private static int[][] listToInt(
			List<SignedIntArray> list ) {
		IntegratedIntCompressor iic = new IntegratedIntCompressor();
		int[][] retVal = new int[list.size()][];
		for (int i = 0; i < list.size(); i++) {
			List<Integer> internalList = list.get(
					i).getSamplesList();
			retVal[i] = iic.uncompress(ArrayUtils.toPrimitive(internalList.toArray(new Integer[internalList.size()])));
		}
		return retVal;
	}

	private static int[][] shortToInt(
			short[][] shortBank ) {
		int[][] intBank = new int[shortBank.length][];
		for (int a = 0; a < shortBank.length; a++) {
			intBank[a] = new int[shortBank[a].length];
			for (int i = 0; i < shortBank[a].length; i++) {
				intBank[a][i] = shortBank[a][i];
			}
		}

		return intBank;
	}

	private static short[][] intToShort(
			int[][] intBank ) {
		short[][] shortBank = new short[intBank.length][];
		for (int a = 0; a < intBank.length; a++) {
			shortBank[a] = new short[intBank[a].length];
			for (int i = 0; i < intBank[a].length; i++) {
				shortBank[a][i] = (short) intBank[a][i];
			}
		}
		return shortBank;
	}

	@Override
	public byte[] toBinary() {
		final byte[] dataBufferBinary = getDataBufferBinary(dataBuffer);
		byte[] metadataBytes;
		if (metadata != null) {
			metadataBytes = PersistenceUtils.toBinary(metadata);
		}
		else {
			metadataBytes = new byte[] {};
		}
		final ByteBuffer buf = ByteBuffer.allocate(metadataBytes.length + dataBufferBinary.length + 4);
		buf.putInt(metadataBytes.length);
		buf.put(metadataBytes);
		buf.put(dataBufferBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		try {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			final int metadataLength = buf.getInt();
			if (metadataLength > 0) {
				final byte[] metadataBytes = new byte[metadataLength];
				buf.get(metadataBytes);
				metadata = (T) PersistenceUtils.fromBinary(metadataBytes);
			}
			final byte[] dataBufferBytes = new byte[bytes.length - metadataLength - 4];
			buf.get(dataBufferBytes);
			dataBuffer = getDataBuffer(dataBufferBytes);
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to deserialize data buffer",
					e);
		}
	}

	public void setDataBuffer(
			final DataBuffer dataBuffer ) {
		this.dataBuffer = dataBuffer;
	}

	public void setMetadata(
			final T metadata ) {
		this.metadata = metadata;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		// This will get wrapped as a MergeableRasterTile by the combiner to
		// support merging
	}
	// public static byte[] writeDataBuffer(DataBuffer buffer)
	// throws IOException {
	// ByteBuffer buf = ByteBuffer.allocate(10);
	// buf
	// .put(
	// (byte)buffer.getDataType());
	// LZ4Factory factory = LZ4Factory.fastestInstance();
	// buf
	// .put(
	// buffer.getOffsets());
	// out
	// .writeInt(
	// dataBuffer.getSize());
	// Object dataArray = null;
	// switch (dataType) {
	// case DataBuffer.TYPE_BYTE:
	// dataArray = ((DataBufferByte) dataBuffer).getBankData();
	// break;
	// case DataBuffer.TYPE_SHORT:
	// dataArray = ((DataBufferShort) dataBuffer).getBankData();
	// break;
	// case DataBuffer.TYPE_USHORT:
	// dataArray = ((DataBufferUShort) dataBuffer).getBankData();
	// break;
	// case DataBuffer.TYPE_INT:
	// dataArray = ((DataBufferInt) dataBuffer).getBankData();
	// break;
	// case DataBuffer.TYPE_FLOAT:
	// dataArray = ((DataBufferFloat) dataBuffer)
	// .getBankData(
	// );
	// break;
	// case DataBuffer.TYPE_DOUBLE:
	// dataArray = ((DataBufferDouble) dataBuffer)
	// .getBankData(
	// );
	// break;
	// default:
	// throw new RuntimeException();
	// }
	// out
	// .writeObject(
	// dataArray);
	// }
	//
	// /**
	// * Deserialize the <code>DataBufferState</code>.
	// *
	// * @param out The <code>ObjectInputStream</code>.
	// */
	// private void readObject(ObjectInputStream in)
	// throws IOException, ClassNotFoundException {
	// DataBuffer dataBuffer = null;
	//
	// // Read serialized form from the stream.
	// int dataType = -1;
	// int[] offsets = null;
	// int size = -1;
	// Object dataArray = null;
	// dataType = in.readInt();
	// offsets = (int[])in.readObject();
	// size = in.readInt();
	// dataArray = in.readObject();
	//
	// // Restore the transient DataBuffer.
	// switch (dataType) {
	// case DataBuffer.TYPE_BYTE:
	// dataBuffer =
	// new DataBufferByte((byte[][])dataArray, size, offsets);
	// break;
	// case DataBuffer.TYPE_SHORT:
	// dataBuffer =
	// new DataBufferShort((short[][])dataArray, size, offsets);
	// break;
	// case DataBuffer.TYPE_USHORT:
	// dataBuffer =
	// new DataBufferUShort((short[][])dataArray, size, offsets);
	// break;
	// case DataBuffer.TYPE_INT:
	// dataBuffer =
	// new DataBufferInt((int[][])dataArray, size, offsets);
	// break;
	// case DataBuffer.TYPE_FLOAT:
	// dataBuffer =
	// new DataBufferFloat((float[][])dataArray, size, offsets);
	// break;
	// case DataBuffer.TYPE_DOUBLE:
	// dataBuffer =
	// new DataBufferDouble((double[][])dataArray, size, offsets);
	// break;
	// default:
	// throw new RuntimeException();
	// }
	//
	// theObject = dataBuffer;
}
