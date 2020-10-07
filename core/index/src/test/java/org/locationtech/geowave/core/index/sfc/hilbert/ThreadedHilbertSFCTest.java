/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.sfc.hilbert;

// import com.vmlens.api.AllInterleavings;
import org.junit.Test;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory;
import org.locationtech.geowave.core.index.sfc.SpaceFillingCurve;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;

public class ThreadedHilbertSFCTest {

  @Test
  public void testDecomposeQuery_2DSpatialOneIndexFilter() throws InterruptedException {
    final int LATITUDE_BITS = 50;
    final int LONGITUDE_BITS = 50;

    final SFCDimensionDefinition[] SPATIAL_DIMENSIONS =
        new SFCDimensionDefinition[] {
            new SFCDimensionDefinition(new BasicDimensionDefinition(-180, 180), LONGITUDE_BITS),
            new SFCDimensionDefinition(new BasicDimensionDefinition(-90, 90), LATITUDE_BITS)};

    final SpaceFillingCurve hilbertSFC =
        SFCFactory.createSpaceFillingCurve(SPATIAL_DIMENSIONS, SFCFactory.SFCType.HILBERT);

    final BasicNumericDataset spatialQuery1 =
        new BasicNumericDataset(
            new NumericData[] {new NumericRange(55, 57), new NumericRange(25, 27)});

    final BasicNumericDataset spatialQuery2 =
        new BasicNumericDataset(new NumericData[] {new NumericRange(5, 7), new NumericRange(5, 7)});

    // try (AllInterleavings allInterleavings = new AllInterleavings("ThreadedHilbertTest");) {

    //   while (allInterleavings.hasNext()) {

    //     Thread first = new Thread(() -> {
    //       hilbertSFC.decomposeRange(spatialQuery1, false, 2);
    //     });

    //     Thread second = new Thread(() -> {
    //       hilbertSFC.decomposeRange(spatialQuery2, true, 8);
    //     });

    //     first.start();
    //     second.start();

    //     first.join();
    //     second.join();
    //   }
    // }

  }
}
