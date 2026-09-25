/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.util;

import java.awt.image.BandedSampleModel;
import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.SampleModel;
import java.awt.image.SinglePixelPackedSampleModel;
import org.eclipse.imagen.ComponentSampleModelImageN;
import org.locationtech.geowave.adapter.raster.protobuf.SampleModelProtos;
import com.google.common.primitives.Ints;
import com.google.protobuf.InvalidProtocolBufferException;

public class SampleModelPersistenceUtils {

  /** Flag indicating a BandedSampleModel. */
  private static final int TYPE_BANDED = 1;

  /** Flag indicating a PixelInterleavedSampleModel. */
  private static final int TYPE_PIXEL_INTERLEAVED = 2;

  /** Flag indicating a SinglePixelPackedSampleModel. */
  private static final int TYPE_SINGLE_PIXEL_PACKED = 3;

  /** Flag indicating a MultiPixelPackedSampleModel. */
  private static final int TYPE_MULTI_PIXEL_PACKED = 4;

  /** Flag indicating a ComponentSampleModelImageN. */
  private static final int TYPE_COMPONENT_JAI = 5;

  /** Flag indicating a generic ComponentSampleModel. */
  private static final int TYPE_COMPONENT = 6;

  public static byte[] getSampleModelBinary(final SampleModel sampleModel) {
    final SampleModelProtos.SampleModel.Builder bldr = SampleModelProtos.SampleModel.newBuilder();
    if (sampleModel instanceof ComponentSampleModel) {
      final ComponentSampleModel sm = (ComponentSampleModel) sampleModel;
      int sampleModelType = TYPE_COMPONENT;
      final int transferType = sm.getTransferType();
      if (sampleModel instanceof PixelInterleavedSampleModel) {
        sampleModelType = TYPE_PIXEL_INTERLEAVED;
      } else if (sampleModel instanceof BandedSampleModel) {
        sampleModelType = TYPE_BANDED;
      } else if ((sampleModel instanceof ComponentSampleModelImageN)
          || (transferType == DataBuffer.TYPE_FLOAT)
          || (transferType == DataBuffer.TYPE_DOUBLE)) {
        sampleModelType = TYPE_COMPONENT_JAI;
      }
      bldr.setModelType(sampleModelType);
      if (sampleModelType != TYPE_BANDED) {
        bldr.setPixelStride(sm.getPixelStride());
      }
      bldr.setScanlineStride(sm.getScanlineStride());
      if (sampleModelType != TYPE_PIXEL_INTERLEAVED) {
        bldr.addAllBankIndices(Ints.asList(sm.getBankIndices()));
      }
      bldr.addAllBandOffsets(Ints.asList(sm.getBandOffsets()));
    } else if (sampleModel instanceof SinglePixelPackedSampleModel) {
      final SinglePixelPackedSampleModel sm = (SinglePixelPackedSampleModel) sampleModel;
      bldr.setModelType(TYPE_SINGLE_PIXEL_PACKED);
      bldr.setScanlineStride(sm.getScanlineStride());
      bldr.addAllBitMasks(Ints.asList(sm.getBitMasks()));
    } else if (sampleModel instanceof MultiPixelPackedSampleModel) {
      final MultiPixelPackedSampleModel sm = (MultiPixelPackedSampleModel) sampleModel;
      bldr.setModelType(TYPE_MULTI_PIXEL_PACKED);
      bldr.setPixelBitStride(sm.getPixelBitStride());
      bldr.setScanlineStride(sm.getScanlineStride());
      bldr.setDataBitOffset(sm.getDataBitOffset());
    } else {
      throw new RuntimeException("Unsupported SampleModel type for serialization " + sampleModel);
    }

    bldr.setTransferType(sampleModel.getTransferType());
    bldr.setWidth(sampleModel.getWidth());
    bldr.setHeight(sampleModel.getHeight());
    return bldr.build().toByteArray();
  }

  public static SampleModel getSampleModel(final byte[] binary)
      throws InvalidProtocolBufferException {
    final SampleModelProtos.SampleModel sm = SampleModelProtos.SampleModel.parseFrom(binary);
    final int sampleModelType = sm.getModelType();
    switch (sampleModelType) {
      case TYPE_PIXEL_INTERLEAVED:
        return createPixelInterleavedSampleModel(
            sm.getTransferType(),
            sm.getWidth(),
            sm.getHeight(),
            sm.getPixelStride(),
            sm.getScanlineStride(),
            DataBufferPersistenceUtils.integerListToPrimitiveArray(sm.getBandOffsetsList()));
      case TYPE_BANDED:
        return createBandedSampleModel(
            sm.getTransferType(),
            sm.getWidth(),
            sm.getHeight(),
            sm.getScanlineStride(),
            DataBufferPersistenceUtils.integerListToPrimitiveArray(sm.getBankIndicesList()),
            DataBufferPersistenceUtils.integerListToPrimitiveArray(sm.getBandOffsetsList()));
      case TYPE_COMPONENT_JAI:
        return new ComponentSampleModelImageN(
            sm.getTransferType(),
            sm.getWidth(),
            sm.getHeight(),
            sm.getPixelStride(),
            sm.getScanlineStride(),
            DataBufferPersistenceUtils.integerListToPrimitiveArray(sm.getBankIndicesList()),
            DataBufferPersistenceUtils.integerListToPrimitiveArray(sm.getBandOffsetsList()));
      case TYPE_COMPONENT:
        return new ComponentSampleModel(
            sm.getTransferType(),
            sm.getWidth(),
            sm.getHeight(),
            sm.getPixelStride(),
            sm.getScanlineStride(),
            DataBufferPersistenceUtils.integerListToPrimitiveArray(sm.getBankIndicesList()),
            DataBufferPersistenceUtils.integerListToPrimitiveArray(sm.getBandOffsetsList()));
      case TYPE_SINGLE_PIXEL_PACKED:
        return new SinglePixelPackedSampleModel(
            sm.getTransferType(),
            sm.getWidth(),
            sm.getHeight(),
            sm.getScanlineStride(),
            DataBufferPersistenceUtils.integerListToPrimitiveArray(sm.getBitMasksList()));
      case TYPE_MULTI_PIXEL_PACKED:
        return new MultiPixelPackedSampleModel(
            sm.getTransferType(),
            sm.getWidth(),
            sm.getHeight(),
            sm.getPixelBitStride(),
            sm.getScanlineStride(),
            sm.getDataBitOffset());
      default:
        throw new RuntimeException(
            "Unsupported sample model type for deserialization " + sampleModelType);
    }
  }

  private static SampleModel createBandedSampleModel(
      final int dataType,
      final int width,
      final int height,
      final int numBands,
      int bankIndices[],
      int bandOffsets[]) {
    if (numBands < 1) {
      throw new IllegalArgumentException("Num Bands must be >= 1");
    }
    if (bankIndices == null) {
      bankIndices = new int[numBands];
      for (int i = 0; i < numBands; i++) {
        bankIndices[i] = i;
      }
    }
    if (bandOffsets == null) {
      bandOffsets = new int[numBands];
      for (int i = 0; i < numBands; i++) {
        bandOffsets[i] = 0;
      }
    }
    if (bandOffsets.length != bankIndices.length) {
      throw new IllegalArgumentException(
          "Band Offsets "
              + bandOffsets.length
              + " doesn't match Bank Indices "
              + bankIndices.length);
    }
    return new ComponentSampleModelImageN(
        dataType,
        width,
        height,
        1,
        width,
        bankIndices,
        bandOffsets);
  }

  private static SampleModel createPixelInterleavedSampleModel(
      final int dataType,
      final int width,
      final int height,
      final int pixelStride,
      final int scanlineStride,
      final int bandOffsets[]) {
    if (bandOffsets == null) {
      throw new IllegalArgumentException();
    }
    int minBandOff = bandOffsets[0];
    int maxBandOff = bandOffsets[0];
    for (int i = 1; i < bandOffsets.length; i++) {
      minBandOff = Math.min(minBandOff, bandOffsets[i]);
      maxBandOff = Math.max(maxBandOff, bandOffsets[i]);
    }
    maxBandOff -= minBandOff;
    if (maxBandOff > scanlineStride) {
      throw new IllegalArgumentException(
          "max Band Offset ("
              + maxBandOff
              + ") must be > scanline stride ("
              + scanlineStride
              + ")");
    }
    if ((pixelStride * width) > scanlineStride) {
      throw new IllegalArgumentException(
          "pixelStride*width ("
              + (pixelStride * width)
              + ") must be > scanline stride ("
              + scanlineStride
              + ")");
    }
    if (pixelStride < maxBandOff) {
      throw new IllegalArgumentException(
          "max Band Offset (" + maxBandOff + ") must be > pixel stride (" + pixelStride + ")");
    }

    switch (dataType) {
      case DataBuffer.TYPE_BYTE:
      case DataBuffer.TYPE_USHORT:
        return new PixelInterleavedSampleModel(
            dataType,
            width,
            height,
            pixelStride,
            scanlineStride,
            bandOffsets);
      case DataBuffer.TYPE_INT:
      case DataBuffer.TYPE_SHORT:
      case DataBuffer.TYPE_FLOAT:
      case DataBuffer.TYPE_DOUBLE:
        return new ComponentSampleModelImageN(
            dataType,
            width,
            height,
            pixelStride,
            scanlineStride,
            bandOffsets);
      default:
        throw new IllegalArgumentException("Unsupported data buffer type");
    }
  }
}
