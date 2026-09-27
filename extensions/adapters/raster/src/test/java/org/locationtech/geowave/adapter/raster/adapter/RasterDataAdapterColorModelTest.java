/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.awt.color.ColorSpace;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.IndexColorModel;
import java.util.Base64;
import org.eclipse.imagen.NotAColorSpace;
import org.junit.Test;

public class RasterDataAdapterColorModelTest {
  // What getColorModelBinary wrote on JAI, before the move to ImageN, for the color model
  // RasterUtils.createDataAdapterTypeDouble gives a 3-band adapter.
  private static final String JAI_DOUBLE_COMPONENT =
      "rO0ABXNyACVjb20uc3VuLm1lZGlhLmphaS5ybWkuQ29sb3JNb2RlbFN0YXRlSr2IbyYuFVQDAAB4"
          + "cgArY29tLnN1bi5tZWRpYS5qYWkucm1pLlNlcmlhbGl6YWJsZVN0YXRlSW1wbACZn285WhS6AgAB"
          + "TAAIdGhlQ2xhc3N0ABFMamF2YS9sYW5nL0NsYXNzO3hwdnIAImphdmEuYXd0LmltYWdlLkNvbXBv"
          + "bmVudENvbG9yTW9kZWwAAAAAAAAAAAAAAHhwdwkAAAACAAAAAABzcgAwY29tLnN1bi5tZWRpYS5p"
          + "bWFnZWlvaW1wbC5jb21tb24uQm9ndXNDb2xvclNwYWNluDYQbOR0BjECAAB4cgAZamF2YS5hd3Qu"
          + "Y29sb3IuQ29sb3JTcGFjZfpRVO9PW4TEAgACSQANbnVtQ29tcG9uZW50c0kABHR5cGV4cAAAAAMA"
          + "AAANdXIAAltJTbpgJnbqsqUCAAB4cAAAAAMAAABAAAAAQAAAAEB3CgAAAAAAAQAAAAV4";
  // An 8-bit sRGB ComponentColorModel, as written on JAI.
  private static final String JAI_SRGB_COMPONENT =
      "rO0ABXNyACVjb20uc3VuLm1lZGlhLmphaS5ybWkuQ29sb3JNb2RlbFN0YXRlSr2IbyYuFVQDAAB4"
          + "cgArY29tLnN1bi5tZWRpYS5qYWkucm1pLlNlcmlhbGl6YWJsZVN0YXRlSW1wbACZn285WhS6AgAB"
          + "TAAIdGhlQ2xhc3N0ABFMamF2YS9sYW5nL0NsYXNzO3hwdnIAImphdmEuYXd0LmltYWdlLkNvbXBv"
          + "bmVudENvbG9yTW9kZWwAAAAAAAAAAAAAAHhwdwwAAAACAAAAAQAAA+h1cgACW0lNumAmduqypQIA"
          + "AHhwAAAAAwAAAAgAAAAIAAAACHcKAAAAAAABAAAAAHg=";
  // A 3-entry 8-bit IndexColorModel with alpha, as written on JAI.
  private static final String JAI_INDEX =
      "rO0ABXNyACVjb20uc3VuLm1lZGlhLmphaS5ybWkuQ29sb3JNb2RlbFN0YXRlSr2IbyYuFVQDAAB4"
          + "cgArY29tLnN1bi5tZWRpYS5qYWkucm1pLlNlcmlhbGl6YWJsZVN0YXRlSW1wbACZn285WhS6AgAB"
          + "TAAIdGhlQ2xhc3N0ABFMamF2YS9sYW5nL0NsYXNzO3hwdnIAHmphdmEuYXd0LmltYWdlLkluZGV4"
          + "Q29sb3JNb2RlbAAAAAAAAAAAAAAAeHB3DAAAAAMAAAAIAAAAA3VyAAJbSU26YCZ26rKlAgAAeHAA"
          + "AAAD/wAAAP//AACAAP8AdwkB/////wAAAAB4";

  @Test
  public void testReadJaiDoubleComponentColorModel() throws Exception {
    final ColorModel colorModel =
        RasterDataAdapter.getColorModel(Base64.getDecoder().decode(JAI_DOUBLE_COMPONENT));
    assertEquals(ComponentColorModel.class, colorModel.getClass());
    assertEquals(NotAColorSpace.class, colorModel.getColorSpace().getClass());
    assertEquals(3, colorModel.getNumComponents());
    assertEquals(DataBuffer.TYPE_DOUBLE, colorModel.getTransferType());
  }

  @Test
  public void testReadJaiSrgbComponentColorModel() throws Exception {
    final ColorModel colorModel =
        RasterDataAdapter.getColorModel(Base64.getDecoder().decode(JAI_SRGB_COMPONENT));
    assertEquals(ComponentColorModel.class, colorModel.getClass());
    assertEquals(ColorSpace.getInstance(ColorSpace.CS_sRGB), colorModel.getColorSpace());
    assertEquals(DataBuffer.TYPE_BYTE, colorModel.getTransferType());
  }

  @Test
  public void testReadJaiIndexColorModel() throws Exception {
    final IndexColorModel colorModel =
        (IndexColorModel) RasterDataAdapter.getColorModel(Base64.getDecoder().decode(JAI_INDEX));
    final int[] rgbs = new int[colorModel.getMapSize()];
    colorModel.getRGBs(rgbs);
    assertArrayEquals(new int[] {0xFF000000, 0xFFFF0000, 0x8000FF00}, rgbs);
  }

  @Test
  public void testRoundTrip() throws Exception {
    final ColorModel original =
        new ComponentColorModel(
            new NotAColorSpace(2),
            new int[] {64, 64},
            false,
            false,
            ColorModel.OPAQUE,
            DataBuffer.TYPE_DOUBLE);
    final ColorModel restored =
        RasterDataAdapter.getColorModel(RasterDataAdapter.getColorModelBinary(original));
    assertEquals(original.getClass(), restored.getClass());
    assertEquals(NotAColorSpace.class, restored.getColorSpace().getClass());
    assertEquals(2, restored.getNumComponents());
    assertEquals(DataBuffer.TYPE_DOUBLE, restored.getTransferType());
  }
}
