/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.BaseDataStoreTest;
import org.locationtech.geowave.adapter.vector.plugin.GeoWavePluginException;
import org.locationtech.geowave.adapter.vector.util.DateUtilities;
import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureTimeRangeStatistics;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class TemporalRangeTest extends BaseDataStoreTest {
  DataStore dataStore;
  SimpleFeatureType type;
  GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));

  @Before
  public void setup() throws SchemaException, CQLException, IOException, GeoWavePluginException {
    dataStore = createDataStore();
    type =
        DataUtilities.createType(
            "geostuff",
            "geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String,when:Date");

    dataStore.createSchema(type);
  }

  @Test
  public void test() throws ParseException, IOException {
    final Calendar gmt = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    final Calendar local = Calendar.getInstance(TimeZone.getTimeZone("EDT"));
    local.setTimeInMillis(gmt.getTimeInMillis());
    final TemporalRange rGmt = new TemporalRange(gmt.getTime(), gmt.getTime());
    final TemporalRange rLocal = new TemporalRange(local.getTime(), local.getTime());
    rGmt.fromBinary(rGmt.toBinary());
    assertEquals(gmt.getTime(), rGmt.getEndTime());
    assertEquals(rLocal.getEndTime(), rGmt.getEndTime());
    assertEquals(rLocal.getEndTime().getTime(), rGmt.getEndTime().getTime());

    final Transaction transaction1 = new DefaultTransaction();

    final FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
        dataStore.getFeatureWriter(type.getTypeName(), transaction1);
    final SimpleFeature newFeature = writer.next();
    newFeature.setAttribute("pop", Long.valueOf(77));
    newFeature.setAttribute("pid", UUID.randomUUID().toString());
    newFeature.setAttribute("when", DateUtilities.parseISO("2005-05-19T19:32:56-04:00"));
    newFeature.setAttribute("geometry", factory.createPoint(new Coordinate(43.454, 28.232)));

    final FeatureTimeRangeStatistics stats = new FeatureTimeRangeStatistics(null, "when");
    stats.entryIngested(newFeature);

    assertEquals(
        DateUtilities.parseISO("2005-05-19T23:32:56Z"),
        stats.asTemporalRange().getStartTime());
  }
}
