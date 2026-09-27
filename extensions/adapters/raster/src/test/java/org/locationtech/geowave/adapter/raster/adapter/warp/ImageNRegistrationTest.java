/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter.warp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import org.eclipse.imagen.ImageN;
import org.eclipse.imagen.Interpolation;
import org.eclipse.imagen.OperationDescriptor;
import org.eclipse.imagen.ParameterBlockImageN;
import org.eclipse.imagen.WarpAffine;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geowave.adapter.raster.adapter.SourceThresholdFixMosaicDescriptor;

public class ImageNRegistrationTest {

  @BeforeClass
  public static void register() {
    SourceThresholdFixMosaicDescriptor.register(false);
    WarpRIF.register(false);
  }

  @Test
  public void testNearestNeighborWarpUsesGeoWaveOperator() {
    final ParameterBlockImageN pb = new ParameterBlockImageN("Warp");
    pb.addSource(new BufferedImage(16, 16, BufferedImage.TYPE_BYTE_GRAY));
    pb.setParameter("warp", new WarpAffine(AffineTransform.getScaleInstance(0.5, 0.5)));
    pb.setParameter("interpolation", Interpolation.getInstance(Interpolation.INTERP_NEAREST));
    assertTrue(ImageN.create("Warp", pb).getRendering() instanceof WarpNearestOpImage);
  }

  @Test
  public void testMosaicUsesGeoWaveSourceThreshold() {
    final OperationDescriptor mosaic =
        (OperationDescriptor) ImageN.getDefaultInstance().getOperationRegistry().getDescriptor(
            "rendered",
            "Mosaic");
    assertEquals(SourceThresholdFixMosaicDescriptor.class, mosaic.getClass());
    final double[][] threshold =
        (double[][]) mosaic.getParameterListDescriptor("rendered").getParamDefaults()[3];
    assertArrayEquals(new double[] {Double.MIN_VALUE}, threshold[0], 0);
  }
}
