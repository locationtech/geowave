/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.util;

import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferDouble;
import java.awt.image.DataBufferFloat;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.ByteDataBuffer;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.DoubleArray;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.DoubleDataBuffer;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.FloatArray;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.FloatDataBuffer;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.SignedIntArray;
import org.locationtech.geowave.adapter.raster.protobuf.DataBufferProtos.SignedIntDataBuffer;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import me.lemire.integercompression.differential.IntegratedIntCompressor;

public class DataBufferPersistenceUtils {
  public static byte[] getDataBufferBinary(final DataBuffer dataBuffer) {
    final DataBufferProtos.DataBuffer.Builder bldr = DataBufferProtos.DataBuffer.newBuilder();
    bldr.setType(dataBuffer.getDataType());
    bldr.addAllOffsets(Ints.asList(dataBuffer.getOffsets()));
    bldr.setSize(dataBuffer.getSize());
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
            return ByteString.copyFrom(byteBank[index++]);
          }
        };
        byteBldr.addAllBanks(byteIt);
        bldr.setByteDb(byteBldr.build());
        break;
      case DataBuffer.TYPE_SHORT:
        setBuilder(shortToInt(((DataBufferShort) dataBuffer).getBankData()), bldr);
        break;
      case DataBuffer.TYPE_USHORT:
        setBuilder(shortToInt(((DataBufferUShort) dataBuffer).getBankData()), bldr);
        break;
      case DataBuffer.TYPE_INT:
        setBuilder(((DataBufferInt) dataBuffer).getBankData(), bldr);
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
            return FloatArray.newBuilder().addAllSamples(Floats.asList(fltBank[index++])).build();
          }
        };
        fltBldr.addAllBanks(floatIt);
        bldr.setFlt(fltBldr);
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
            return DoubleArray.newBuilder().addAllSamples(Doubles.asList(dblBank[index++])).build();
          }
        };
        dblBldr.addAllBanks(dblIt);
        bldr.setDbl(dblBldr);
        break;
      default:
        throw new RuntimeException(
            "Unsupported DataBuffer type for serialization " + dataBuffer.getDataType());
    }
    return bldr.build().toByteArray();
  }

  private static void setBuilder(
      final int[][] intBank,
      final DataBufferProtos.DataBuffer.Builder bldr) {
    final IntegratedIntCompressor iic = new IntegratedIntCompressor();
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
        final int[] compressed = iic.compress(internalArray);
        return SignedIntArray.newBuilder().addAllSamples(Ints.asList(compressed)).build();
      }
    };
    intBldr.addAllBanks(intIt);
    bldr.setSint(intBldr);
  }

  public static DataBuffer getDataBuffer(final byte[] binary)
      throws IOException, ClassNotFoundException {
    // // Read serialized form from the stream.
    final DataBufferProtos.DataBuffer buffer = DataBufferProtos.DataBuffer.parseFrom(binary);

    final int[] offsets = ArrayUtils.toPrimitive(buffer.getOffsetsList().toArray(new Integer[] {}));
    // Restore the transient DataBuffer.
    switch (buffer.getType()) {
      case DataBuffer.TYPE_BYTE:
        return new DataBufferByte(
            listToByte(buffer.getByteDb().getBanksList()),
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
        throw new RuntimeException(
            "Unsupported data buffer type for deserialization" + buffer.getType());
    }
  }

  private static byte[][] listToByte(final List<ByteString> list) {
    final byte[][] retVal = new byte[list.size()][];
    for (int i = 0; i < list.size(); i++) {
      retVal[i] = list.get(i).toByteArray();
    }
    return retVal;
  }

  private static float[][] listToFloat(final List<FloatArray> list) {
    final float[][] retVal = new float[list.size()][];
    for (int i = 0; i < list.size(); i++) {
      final List<Float> internalList = list.get(i).getSamplesList();
      retVal[i] = ArrayUtils.toPrimitive(internalList.toArray(new Float[internalList.size()]));
    }
    return retVal;
  }

  private static double[][] listToDouble(final List<DoubleArray> list) {
    final double[][] retVal = new double[list.size()][];
    for (int i = 0; i < list.size(); i++) {
      final List<Double> internalList = list.get(i).getSamplesList();
      retVal[i] = ArrayUtils.toPrimitive(internalList.toArray(new Double[internalList.size()]));
    }
    return retVal;
  }

  private static int[][] listToInt(final List<SignedIntArray> list) {
    final IntegratedIntCompressor iic = new IntegratedIntCompressor();
    final int[][] retVal = new int[list.size()][];
    for (int i = 0; i < list.size(); i++) {
      final List<Integer> internalList = list.get(i).getSamplesList();
      retVal[i] = iic.uncompress(integerListToPrimitiveArray(internalList));
    }
    return retVal;
  }

  protected static int[] integerListToPrimitiveArray(final List<Integer> internalList) {
    return ArrayUtils.toPrimitive(internalList.toArray(new Integer[internalList.size()]));
  }

  private static int[][] shortToInt(final short[][] shortBank) {
    final int[][] intBank = new int[shortBank.length][];
    for (int a = 0; a < shortBank.length; a++) {
      intBank[a] = new int[shortBank[a].length];
      for (int i = 0; i < shortBank[a].length; i++) {
        intBank[a][i] = shortBank[a][i];
      }
    }

    return intBank;
  }

  private static short[][] intToShort(final int[][] intBank) {
    final short[][] shortBank = new short[intBank.length][];
    for (int a = 0; a < intBank.length; a++) {
      shortBank[a] = new short[intBank[a].length];
      for (int i = 0; i < intBank[a].length; i++) {
        shortBank[a][i] = (short) intBank[a][i];
      }
    }
    return shortBank;
  }
}
