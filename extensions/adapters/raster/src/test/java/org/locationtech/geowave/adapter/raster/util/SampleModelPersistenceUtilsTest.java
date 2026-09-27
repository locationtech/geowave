/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import org.eclipse.imagen.ComponentSampleModelImageN;
import org.junit.Test;

public class SampleModelPersistenceUtilsTest {

  @Test
  public void testFloatingPointComponentSampleModelRoundTrip() throws Exception {
    for (final int dataType : new int[] {DataBuffer.TYPE_FLOAT, DataBuffer.TYPE_DOUBLE}) {
      final SampleModel original =
          new ComponentSampleModelImageN(dataType, 4, 3, 1, 4, new int[] {0, 1}, new int[] {0, 0});
      final byte[] binary = SampleModelPersistenceUtils.getSampleModelBinary(original);
      final SampleModel restored = SampleModelPersistenceUtils.getSampleModel(binary);

      assertEquals(ComponentSampleModelImageN.class, restored.getClass());
      assertArrayEquals(binary, SampleModelPersistenceUtils.getSampleModelBinary(restored));

      final DataBuffer data = original.createDataBuffer();
      for (int y = 0; y < 3; y++) {
        for (int x = 0; x < 4; x++) {
          original.setSample(x, y, 0, (x * 10.5) + y, data);
          original.setSample(x, y, 1, -x - (y * 0.25), data);
        }
      }
      for (int y = 0; y < 3; y++) {
        for (int x = 0; x < 4; x++) {
          assertArrayEquals(
              original.getPixel(x, y, (double[]) null, data),
              restored.getPixel(x, y, (double[]) null, data),
              0);
        }
      }
    }
  }
}
