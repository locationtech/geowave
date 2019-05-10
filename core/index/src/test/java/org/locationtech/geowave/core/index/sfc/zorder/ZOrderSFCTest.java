/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.sfc.zorder;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;

public class ZOrderSFCTest {

    @Test
    public void testIndex() {
        double[] latLngValues = new double[] {45.D, 22.D};
        Assert.assertArrayEquals(new byte[] {12}, createSFC().getId(latLngValues));
    }

    @Test
    public void testGetRanges() {
        double[] latLngValues = new double[] {45.D, 22.D};
        byte[] index = createSFC().getId(latLngValues);
        MultiDimensionalNumericData ranges = createSFC().getRanges(index);
        NumericData[] data = ranges.getDataPerDimension();
        NumericData[] actualDate = new NumericRange[] {
                new NumericRange(0.0, 90.0),
                new NumericRange(0.0, 45.0)
        };
        Assert.assertArrayEquals(data, actualDate);
    }

    private ZOrderSFC createSFC() {
        SFCDimensionDefinition[] dimensions =
                {
                        new SFCDimensionDefinition(new BasicDimensionDefinition(-180.0, 180.0), 2),
                        new SFCDimensionDefinition(new BasicDimensionDefinition(-90.0, 90.0), 2)};
        return new ZOrderSFC(dimensions);
    }
}
