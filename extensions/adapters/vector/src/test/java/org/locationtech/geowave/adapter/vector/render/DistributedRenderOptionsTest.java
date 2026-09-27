/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.render;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import java.awt.image.DataBuffer;
import java.awt.image.IndexColorModel;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;

public class DistributedRenderOptionsTest {

  @Test
  public void testPaletteRoundTrip() {
    final int[] rgbs = {0x00000000, 0xFFFF0000, 0x8000FF00, 0xFF0000FF, 0xFFFFFFFF};
    final IndexColorModel palette =
        new IndexColorModel(4, rgbs.length, rgbs, 0, true, 0, DataBuffer.TYPE_BYTE);

    final IndexColorModel restored = roundTrip(palette).getPalette();

    assertEquals(palette.getPixelSize(), restored.getPixelSize());
    assertEquals(palette.getMapSize(), restored.getMapSize());
    assertEquals(palette.hasAlpha(), restored.hasAlpha());
    assertEquals(palette.getTransparentPixel(), restored.getTransparentPixel());
    assertEquals(palette.getTransferType(), restored.getTransferType());
    final int[] expected = new int[rgbs.length];
    final int[] actual = new int[rgbs.length];
    palette.getRGBs(expected);
    restored.getRGBs(actual);
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testNoPalette() {
    assertNull(roundTrip(null).getPalette());
  }

  private static DistributedRenderOptions roundTrip(final IndexColorModel palette) {
    final DistributedRenderOptions options = new DistributedRenderOptions();
    options.setEnvelope(new ReferencedEnvelope(-10, 10, -5, 5, GeometryUtils.getDefaultCRS()));
    options.setMapWidth(256);
    options.setMapHeight(128);
    options.setPalette(palette);
    options.setMaxErrors(3);
    final DistributedRenderOptions restored = new DistributedRenderOptions();
    restored.fromBinary(options.toBinary());
    assertEquals(3, restored.getMaxErrors());
    assertEquals(256, restored.getMapWidth());
    return restored;
  }
}
