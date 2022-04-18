/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.filter.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptorBuilder;
import org.locationtech.geowave.core.geotime.adapter.TemporalFieldDescriptorBuilder;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.BBox;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Crosses;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Disjoint;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Intersects;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Overlaps;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.PreparedFilterGeometry;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialContains;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialNotEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Touches;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.UnpreparedFilterGeometry;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Within;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.After;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.Before;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.BeforeOrDuring;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.During;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.DuringOrAfter;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalBetween;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TimeOverlaps;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.AbstractDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.threeten.extra.Interval;

public class SpatialTemporalFilterExpressionTest {

  @Test
  public void testSpatialExpressions() {
    final DataTypeAdapter<TestType> adapter = new TestTypeBasicDataAdapter();
    final TestType entry =
        new TestType(
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(20, 20)),
            new Date(),
            "test");
    final TestType entryNulls = new TestType(null, null, null);
    final SpatialLiteral bboxLit = SpatialLiteral.of(new Envelope(0, 5, 0, 5));
    final SpatialLiteral preparedBboxLit = SpatialLiteral.of(new Envelope(0, 5, 0, 5));
    preparedBboxLit.prepare(GeometryUtils.getDefaultCRS());
    final SpatialLiteral polygonLit =
        SpatialLiteral.of(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(0, 5),
                    new Coordinate(0, 10),
                    new Coordinate(5, 10),
                    new Coordinate(5, 5),
                    new Coordinate(0, 5)}));
    final SpatialLiteral preparedPolygonLit =
        SpatialLiteral.of(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(0, 5),
                    new Coordinate(0, 10),
                    new Coordinate(5, 10),
                    new Coordinate(5, 5),
                    new Coordinate(0, 5)}));
    preparedPolygonLit.prepare(GeometryUtils.getDefaultCRS());
    final SpatialLiteral referencedBboxLit =
        SpatialLiteral.of(new ReferencedEnvelope(0, 25, 0, 25, GeometryUtils.getDefaultCRS()));
    final SpatialLiteral referencedBboxLit2 =
        SpatialLiteral.of(new ReferencedEnvelope(4, 25, 4, 25, GeometryUtils.getDefaultCRS()));
    final SpatialFieldValue spatialField = SpatialFieldValue.of("geom");

    // Test comparisons
    assertTrue(
        bboxLit.isEqualTo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(0, 0),
                    new Coordinate(0, 5),
                    new Coordinate(5, 5),
                    new Coordinate(5, 0),
                    new Coordinate(0, 0)})).evaluate(adapter, entry));
    assertTrue(bboxLit.isEqualTo(preparedBboxLit).evaluate(adapter, entry));
    assertTrue(preparedBboxLit.isEqualTo(bboxLit).evaluate(adapter, entry));
    assertTrue(bboxLit.isEqualTo(new Envelope(0, 5, 0, 5)).evaluate(adapter, entry));
    assertFalse(spatialField.isEqualTo(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(spatialField.isEqualTo(null).evaluate(adapter, entry));
    assertTrue(spatialField.isEqualTo(null).evaluate(adapter, entryNulls));
    assertFalse(spatialField.isEqualTo(bboxLit).evaluate(adapter, entryNulls));
    assertFalse(spatialField.isNull().evaluate(adapter, entry));
    assertTrue(spatialField.isNull().evaluate(adapter, entryNulls));
    assertFalse(
        bboxLit.isNotEqualTo(
            new ReferencedEnvelope(0, 5, 0, 5, GeometryUtils.getDefaultCRS())).evaluate(
                adapter,
                entry));
    assertFalse(bboxLit.isNotEqualTo(preparedBboxLit).evaluate(adapter, entry));
    assertTrue(bboxLit.isNotEqualTo(polygonLit).evaluate(adapter, entry));
    assertFalse(polygonLit.isNotEqualTo(polygonLit).evaluate(adapter, entry));
    assertTrue(spatialField.isNotEqualTo(bboxLit).evaluate(adapter, entryNulls));
    assertFalse(spatialField.isNotEqualTo(null).evaluate(adapter, entryNulls));
    assertTrue(spatialField.isNotEqualTo(null).evaluate(adapter, entry));
    assertTrue(SpatialLiteral.of(null).isNull().evaluate(adapter, entry));

    // Preparing null or already prepared geometries should not fail
    preparedBboxLit.prepare(GeometryUtils.getDefaultCRS());
    SpatialLiteral.of(null).prepare(GeometryUtils.getDefaultCRS());

    try {
      SpatialLiteral.of("invalid");
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    // Test functions
    assertTrue(spatialField.bbox(19, 19, 21, 21).evaluate(adapter, entry));
    assertFalse(spatialField.bbox(0, 0, 5, 5).evaluate(adapter, entry));

    assertTrue(bboxLit.touches(polygonLit).evaluate(adapter, entry));
    assertTrue(preparedBboxLit.touches(polygonLit).evaluate(adapter, entry));
    assertTrue(preparedBboxLit.touches(preparedPolygonLit).evaluate(adapter, entry));
    assertTrue(bboxLit.touches(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(spatialField.touches(polygonLit).evaluate(adapter, entry));
    assertFalse(spatialField.touches(polygonLit).evaluate(adapter, entryNulls));
    assertFalse(polygonLit.touches(spatialField).evaluate(adapter, entryNulls));
    assertFalse(spatialField.touches(preparedPolygonLit).evaluate(adapter, entry));

    assertTrue(bboxLit.intersects(referencedBboxLit).evaluate(adapter, entry));
    assertTrue(preparedBboxLit.intersects(polygonLit).evaluate(adapter, entry));
    assertTrue(preparedBboxLit.intersects(preparedPolygonLit).evaluate(adapter, entry));
    assertTrue(bboxLit.intersects(preparedPolygonLit).evaluate(adapter, entry));
    assertTrue(spatialField.intersects(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(spatialField.intersects(referencedBboxLit).evaluate(adapter, entryNulls));
    assertFalse(polygonLit.intersects(spatialField).evaluate(adapter, entryNulls));
    assertFalse(spatialField.intersects(preparedPolygonLit).evaluate(adapter, entry));

    assertFalse(bboxLit.disjoint(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(preparedBboxLit.disjoint(polygonLit).evaluate(adapter, entry));
    assertFalse(preparedBboxLit.disjoint(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(bboxLit.disjoint(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(spatialField.disjoint(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(spatialField.disjoint(referencedBboxLit).evaluate(adapter, entryNulls));
    assertFalse(polygonLit.disjoint(spatialField).evaluate(adapter, entryNulls));
    assertTrue(spatialField.disjoint(preparedPolygonLit).evaluate(adapter, entry));
    assertTrue(bboxLit.disjoint(spatialField).evaluate(adapter, entry));

    assertFalse(bboxLit.contains(referencedBboxLit).evaluate(adapter, entry));
    assertTrue(referencedBboxLit.contains(bboxLit).evaluate(adapter, entry));
    assertFalse(preparedBboxLit.contains(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(bboxLit.contains(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(spatialField.contains(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(spatialField.contains(referencedBboxLit).evaluate(adapter, entryNulls));
    assertFalse(polygonLit.contains(spatialField).evaluate(adapter, entryNulls));
    assertFalse(spatialField.contains(preparedPolygonLit).evaluate(adapter, entry));
    assertTrue(referencedBboxLit.contains(spatialField).evaluate(adapter, entry));

    assertFalse(bboxLit.crosses(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(referencedBboxLit.crosses(bboxLit).evaluate(adapter, entry));
    assertFalse(preparedBboxLit.crosses(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(bboxLit.crosses(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(spatialField.crosses(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(spatialField.crosses(referencedBboxLit).evaluate(adapter, entryNulls));
    assertFalse(polygonLit.crosses(spatialField).evaluate(adapter, entryNulls));
    assertFalse(spatialField.crosses(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(referencedBboxLit.crosses(spatialField).evaluate(adapter, entry));
    assertTrue(
        SpatialLiteral.of(
            GeometryUtils.GEOMETRY_FACTORY.createLineString(
                new Coordinate[] {new Coordinate(0, 0), new Coordinate(5, 5)})).crosses(
                    SpatialLiteral.of(
                        GeometryUtils.GEOMETRY_FACTORY.createLineString(
                            new Coordinate[] {
                                new Coordinate(5, 0),
                                new Coordinate(0, 5)}))).evaluate(adapter, entry));

    assertTrue(bboxLit.overlaps(referencedBboxLit2).evaluate(adapter, entry));
    assertTrue(referencedBboxLit2.overlaps(bboxLit).evaluate(adapter, entry));
    assertFalse(preparedBboxLit.overlaps(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(bboxLit.overlaps(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(spatialField.overlaps(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(spatialField.overlaps(referencedBboxLit).evaluate(adapter, entryNulls));
    assertFalse(polygonLit.overlaps(spatialField).evaluate(adapter, entryNulls));
    assertFalse(spatialField.overlaps(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(referencedBboxLit.overlaps(spatialField).evaluate(adapter, entry));

    assertTrue(bboxLit.within(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(referencedBboxLit.within(bboxLit).evaluate(adapter, entry));
    assertFalse(preparedBboxLit.within(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(bboxLit.within(preparedPolygonLit).evaluate(adapter, entry));
    assertTrue(spatialField.within(referencedBboxLit).evaluate(adapter, entry));
    assertFalse(spatialField.within(referencedBboxLit).evaluate(adapter, entryNulls));
    assertFalse(polygonLit.within(spatialField).evaluate(adapter, entryNulls));
    assertFalse(spatialField.within(preparedPolygonLit).evaluate(adapter, entry));
    assertFalse(referencedBboxLit.within(spatialField).evaluate(adapter, entry));

    // Test CRS transforms

    // This looks like it should be true, but spatial expressions need to be prepared for the query,
    // the spatial field could be any CRS because it would be determined by the index and not the
    // field descriptor
    assertFalse(
        spatialField.bbox(
            2115070,
            2154935,
            2337709,
            2391878,
            GeometryUtils.decodeCRS("EPSG:3857")).evaluate(adapter, entry));

    // This looks like it should be false, but the expression hasn't been prepared for the query.
    assertTrue(
        spatialField.bbox(0, 0, 556597, 557305, GeometryUtils.decodeCRS("EPSG:3857")).evaluate(
            adapter,
            entry));

    // TODO: add tests for prepared queries where this passes

    try {
      bboxLit.isEqualTo(5).evaluate(adapter, entry);
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    try {
      bboxLit.isNotEqualTo(5).evaluate(adapter, entry);
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    // Test serialization
    byte[] bytes = PersistenceUtils.toBinary(spatialField.bbox(-5, -8, 5, 8));
    final BBox bbox = (BBox) PersistenceUtils.fromBinary(bytes);
    assertTrue(bbox.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) bbox.getExpression1()).getFieldName());
    assertTrue(bbox.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) bbox.getExpression2()).getValue() instanceof UnpreparedFilterGeometry);
    assertTrue(
        ((UnpreparedFilterGeometry) ((SpatialLiteral) bbox.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(-5, 5, -8, 8))));

    bytes = PersistenceUtils.toBinary(spatialField.crosses(bboxLit));
    final Crosses crosses = (Crosses) PersistenceUtils.fromBinary(bytes);
    assertTrue(crosses.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) crosses.getExpression1()).getFieldName());
    assertTrue(crosses.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) crosses.getExpression2()).getValue() instanceof UnpreparedFilterGeometry);
    assertTrue(
        ((UnpreparedFilterGeometry) ((SpatialLiteral) crosses.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(0, 5, 0, 5))));

    bytes = PersistenceUtils.toBinary(spatialField.disjoint(preparedBboxLit));
    final Disjoint disjoint = (Disjoint) PersistenceUtils.fromBinary(bytes);
    assertTrue(disjoint.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) disjoint.getExpression1()).getFieldName());
    assertTrue(disjoint.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) disjoint.getExpression2()).getValue() instanceof PreparedFilterGeometry);
    assertTrue(
        ((PreparedFilterGeometry) ((SpatialLiteral) disjoint.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(0, 5, 0, 5))));

    bytes = PersistenceUtils.toBinary(spatialField.intersects(preparedBboxLit));
    final Intersects intersects = (Intersects) PersistenceUtils.fromBinary(bytes);
    assertTrue(intersects.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) intersects.getExpression1()).getFieldName());
    assertTrue(intersects.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) intersects.getExpression2()).getValue() instanceof PreparedFilterGeometry);
    assertTrue(
        ((PreparedFilterGeometry) ((SpatialLiteral) intersects.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(0, 5, 0, 5))));

    bytes = PersistenceUtils.toBinary(spatialField.overlaps(preparedBboxLit));
    final Overlaps overlaps = (Overlaps) PersistenceUtils.fromBinary(bytes);
    assertTrue(overlaps.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) overlaps.getExpression1()).getFieldName());
    assertTrue(overlaps.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) overlaps.getExpression2()).getValue() instanceof PreparedFilterGeometry);
    assertTrue(
        ((PreparedFilterGeometry) ((SpatialLiteral) overlaps.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(0, 5, 0, 5))));

    bytes = PersistenceUtils.toBinary(spatialField.contains(SpatialLiteral.of(null)));
    final SpatialContains contains = (SpatialContains) PersistenceUtils.fromBinary(bytes);
    assertTrue(contains.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) contains.getExpression1()).getFieldName());
    assertTrue(contains.getExpression2() instanceof SpatialLiteral);
    assertNull(((SpatialLiteral) contains.getExpression2()).getValue());

    bytes = PersistenceUtils.toBinary(spatialField.touches(preparedBboxLit));
    final Touches touches = (Touches) PersistenceUtils.fromBinary(bytes);
    assertTrue(touches.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) touches.getExpression1()).getFieldName());
    assertTrue(touches.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) touches.getExpression2()).getValue() instanceof PreparedFilterGeometry);
    assertTrue(
        ((PreparedFilterGeometry) ((SpatialLiteral) touches.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(0, 5, 0, 5))));

    bytes = PersistenceUtils.toBinary(spatialField.within(preparedBboxLit));
    final Within within = (Within) PersistenceUtils.fromBinary(bytes);
    assertTrue(within.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) within.getExpression1()).getFieldName());
    assertTrue(within.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) within.getExpression2()).getValue() instanceof PreparedFilterGeometry);
    assertTrue(
        ((PreparedFilterGeometry) ((SpatialLiteral) within.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(0, 5, 0, 5))));

    bytes = PersistenceUtils.toBinary(spatialField.isEqualTo(preparedBboxLit));
    final SpatialEqualTo equalTo = (SpatialEqualTo) PersistenceUtils.fromBinary(bytes);
    assertTrue(equalTo.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) equalTo.getExpression1()).getFieldName());
    assertTrue(equalTo.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) equalTo.getExpression2()).getValue() instanceof PreparedFilterGeometry);
    assertTrue(
        ((PreparedFilterGeometry) ((SpatialLiteral) equalTo.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(0, 5, 0, 5))));

    bytes = PersistenceUtils.toBinary(spatialField.isNotEqualTo(preparedBboxLit));
    final SpatialNotEqualTo notEqualTo = (SpatialNotEqualTo) PersistenceUtils.fromBinary(bytes);
    assertTrue(notEqualTo.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) notEqualTo.getExpression1()).getFieldName());
    assertTrue(notEqualTo.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        ((SpatialLiteral) notEqualTo.getExpression2()).getValue() instanceof PreparedFilterGeometry);
    assertTrue(
        ((PreparedFilterGeometry) ((SpatialLiteral) notEqualTo.getExpression2()).getValue()).getGeometry().equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(0, 5, 0, 5))));
  }

  @Test
  public void testTemporalExpressions() {
    final DataTypeAdapter<TestType> adapter = new TestTypeBasicDataAdapter();
    final TestType entry =
        new TestType(
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(20, 20)),
            new Date(500),
            "test");
    final TestType entryNulls = new TestType(null, null, null);
    final TemporalFieldValue dateField = TemporalFieldValue.of("date");
    final TemporalLiteral dateLit = TemporalLiteral.of(new Date(300));
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.setTime(new Date(400));
    final TemporalLiteral calendarLit = TemporalLiteral.of(calendar);
    final TemporalLiteral longLit = TemporalLiteral.of(600);
    final TemporalLiteral instantLit = TemporalLiteral.of(Instant.ofEpochMilli(700));
    final TemporalLiteral intervalLit =
        TemporalLiteral.of(Interval.of(Instant.ofEpochMilli(450), Instant.ofEpochMilli(650)));

    // Test comparisons
    assertTrue(calendarLit.isEqualTo(new Date(400)).evaluate(adapter, entry));
    assertFalse(calendarLit.isEqualTo(dateLit).evaluate(adapter, entry));
    assertTrue(dateField.isEqualTo(new Date(500)).evaluate(adapter, entry));
    assertFalse(dateField.isEqualTo(longLit).evaluate(adapter, entry));
    assertTrue(dateField.isEqualTo(null).evaluate(adapter, entryNulls));
    assertFalse(dateField.isEqualTo(null).evaluate(adapter, entry));
    assertFalse(calendarLit.isNotEqualTo(new Date(400)).evaluate(adapter, entry));
    assertTrue(calendarLit.isNotEqualTo(dateLit).evaluate(adapter, entry));
    assertFalse(dateField.isNotEqualTo(new Date(500)).evaluate(adapter, entry));
    assertTrue(dateField.isNotEqualTo(longLit).evaluate(adapter, entry));
    assertFalse(dateField.isNotEqualTo(null).evaluate(adapter, entryNulls));
    assertTrue(dateField.isNotEqualTo(null).evaluate(adapter, entry));
    assertFalse(dateField.isNull().evaluate(adapter, entry));
    assertTrue(dateField.isNull().evaluate(adapter, entryNulls));
    assertFalse(instantLit.isNull().evaluate(adapter, entry));
    assertFalse(intervalLit.isNull().evaluate(adapter, entry));
    assertTrue(TemporalLiteral.of(null).isNull().evaluate(adapter, entry));
    assertTrue(dateField.isNotNull().evaluate(adapter, entry));
    assertFalse(dateField.isNotNull().evaluate(adapter, entryNulls));
    assertTrue(instantLit.isNotNull().evaluate(adapter, entry));
    assertTrue(intervalLit.isNotNull().evaluate(adapter, entry));
    assertFalse(TemporalLiteral.of(null).isNotNull().evaluate(adapter, entry));
    assertTrue(dateField.isLessThan(longLit).evaluate(adapter, entry));
    assertFalse(dateField.isLessThan(calendarLit).evaluate(adapter, entry));
    assertTrue(calendarLit.isLessThan(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.isLessThan(intervalLit).evaluate(adapter, entry));
    assertTrue(intervalLit.isLessThan(instantLit).evaluate(adapter, entry));
    assertFalse(dateField.isLessThan(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isLessThan(dateField).evaluate(adapter, entryNulls));
    assertTrue(dateField.isLessThanOrEqualTo(longLit).evaluate(adapter, entry));
    assertFalse(dateField.isLessThanOrEqualTo(calendarLit).evaluate(adapter, entry));
    assertTrue(calendarLit.isLessThanOrEqualTo(intervalLit).evaluate(adapter, entry));
    assertTrue(dateField.isLessThanOrEqualTo(intervalLit).evaluate(adapter, entry));
    assertTrue(intervalLit.isLessThanOrEqualTo(instantLit).evaluate(adapter, entry));
    assertFalse(dateField.isLessThanOrEqualTo(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isLessThanOrEqualTo(dateField).evaluate(adapter, entryNulls));
    assertFalse(dateField.isGreaterThan(longLit).evaluate(adapter, entry));
    assertTrue(dateField.isGreaterThan(calendarLit).evaluate(adapter, entry));
    assertFalse(calendarLit.isGreaterThan(intervalLit).evaluate(adapter, entry));
    assertTrue(dateField.isGreaterThan(dateLit).evaluate(adapter, entry));
    assertFalse(intervalLit.isGreaterThan(instantLit).evaluate(adapter, entry));
    assertTrue(instantLit.isGreaterThan(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.isGreaterThan(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isGreaterThan(dateField).evaluate(adapter, entryNulls));
    assertFalse(dateField.isGreaterThanOrEqualTo(longLit).evaluate(adapter, entry));
    assertTrue(dateField.isGreaterThanOrEqualTo(calendarLit).evaluate(adapter, entry));
    assertFalse(calendarLit.isGreaterThanOrEqualTo(intervalLit).evaluate(adapter, entry));
    assertTrue(dateField.isGreaterThanOrEqualTo(dateLit).evaluate(adapter, entry));
    assertFalse(intervalLit.isGreaterThanOrEqualTo(instantLit).evaluate(adapter, entry));
    assertTrue(instantLit.isGreaterThanOrEqualTo(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.isGreaterThanOrEqualTo(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isGreaterThanOrEqualTo(dateField).evaluate(adapter, entryNulls));
    assertTrue(calendarLit.isBetween(dateLit, longLit).evaluate(adapter, entry));
    assertFalse(dateLit.isBetween(calendarLit, longLit).evaluate(adapter, entry));
    assertFalse(longLit.isBetween(dateLit, calendarLit).evaluate(adapter, entry));
    assertFalse(dateField.isBetween(dateLit, longLit).evaluate(adapter, entryNulls));
    assertFalse(dateLit.isBetween(dateField, longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isBetween(dateLit, dateField).evaluate(adapter, entryNulls));
    TemporalBetween between = (TemporalBetween) calendarLit.isBetween(dateField, calendarLit);
    assertTrue(between.getValue() instanceof TemporalLiteral);
    assertTrue(between.getLowerBound() instanceof TemporalFieldValue);
    assertTrue(between.getUpperBound() instanceof TemporalLiteral);

    try {
      dateField.isLessThan("invalid");
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    try {
      dateField.isLessThanOrEqualTo("invalid");
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    try {
      dateField.isGreaterThan("invalid");
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    try {
      dateField.isGreaterThanOrEqualTo("invalid");
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    try {
      dateField.isBetween("invalid", longLit);
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    try {
      dateField.isBetween(longLit, "invalid");
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    try {
      TemporalLiteral.of("invalid");
      fail();
    } catch (RuntimeException e) {
      // expected
    }

    // Test functions
    assertTrue(dateField.isBefore(longLit).evaluate(adapter, entry));
    assertFalse(dateField.isBefore(calendarLit).evaluate(adapter, entry));
    assertTrue(calendarLit.isBefore(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.isBefore(intervalLit).evaluate(adapter, entry));
    assertTrue(intervalLit.isBefore(instantLit).evaluate(adapter, entry));
    assertFalse(dateField.isBefore(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isBefore(dateField).evaluate(adapter, entryNulls));

    assertTrue(dateField.isBeforeOrDuring(longLit).evaluate(adapter, entry));
    assertFalse(dateField.isBeforeOrDuring(calendarLit).evaluate(adapter, entry));
    assertTrue(calendarLit.isBeforeOrDuring(intervalLit).evaluate(adapter, entry));
    assertTrue(dateField.isBeforeOrDuring(intervalLit).evaluate(adapter, entry));
    assertTrue(intervalLit.isBeforeOrDuring(instantLit).evaluate(adapter, entry));
    assertFalse(dateField.isBeforeOrDuring(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isBeforeOrDuring(dateField).evaluate(adapter, entryNulls));

    assertTrue(dateField.isBefore(longLit).evaluate(adapter, entry));
    assertFalse(dateField.isBefore(calendarLit).evaluate(adapter, entry));
    assertTrue(calendarLit.isBefore(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.isBefore(intervalLit).evaluate(adapter, entry));
    assertTrue(intervalLit.isBefore(instantLit).evaluate(adapter, entry));
    assertFalse(dateField.isBefore(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isBefore(dateField).evaluate(adapter, entryNulls));

    assertFalse(dateField.isAfter(longLit).evaluate(adapter, entry));
    assertTrue(dateField.isAfter(calendarLit).evaluate(adapter, entry));
    assertFalse(calendarLit.isAfter(intervalLit).evaluate(adapter, entry));
    assertTrue(dateField.isAfter(dateLit).evaluate(adapter, entry));
    assertFalse(intervalLit.isAfter(instantLit).evaluate(adapter, entry));
    assertTrue(instantLit.isAfter(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.isAfter(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isAfter(dateField).evaluate(adapter, entryNulls));

    assertFalse(dateField.isDuringOrAfter(longLit).evaluate(adapter, entry));
    assertTrue(dateField.isDuringOrAfter(calendarLit).evaluate(adapter, entry));
    assertFalse(calendarLit.isDuringOrAfter(intervalLit).evaluate(adapter, entry));
    assertTrue(dateField.isDuringOrAfter(dateLit).evaluate(adapter, entry));
    assertFalse(intervalLit.isDuringOrAfter(instantLit).evaluate(adapter, entry));
    assertTrue(instantLit.isDuringOrAfter(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.isDuringOrAfter(longLit).evaluate(adapter, entryNulls));
    assertFalse(longLit.isDuringOrAfter(dateField).evaluate(adapter, entryNulls));

    assertFalse(dateField.isDuring(longLit).evaluate(adapter, entry));
    assertFalse(dateField.isDuring(calendarLit).evaluate(adapter, entry));
    assertFalse(calendarLit.isDuring(intervalLit).evaluate(adapter, entry));
    assertTrue(dateField.isDuring(intervalLit).evaluate(adapter, entry));
    assertTrue(longLit.isDuring(intervalLit).evaluate(adapter, entry));
    assertFalse(intervalLit.isDuring(dateField).evaluate(adapter, entry));
    assertFalse(instantLit.isDuring(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.isDuring(intervalLit).evaluate(adapter, entryNulls));
    assertFalse(intervalLit.isDuring(dateField).evaluate(adapter, entryNulls));

    assertFalse(dateField.contains(longLit).evaluate(adapter, entry));
    assertFalse(dateField.contains(calendarLit).evaluate(adapter, entry));
    assertFalse(calendarLit.contains(intervalLit).evaluate(adapter, entry));
    assertTrue(intervalLit.contains(dateField).evaluate(adapter, entry));
    assertTrue(intervalLit.contains(longLit).evaluate(adapter, entry));
    assertFalse(instantLit.contains(intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.contains(intervalLit).evaluate(adapter, entryNulls));
    assertFalse(intervalLit.contains(dateField).evaluate(adapter, entryNulls));

    assertFalse(dateField.overlaps(longLit).evaluate(adapter, entry));
    assertFalse(dateField.overlaps(calendarLit).evaluate(adapter, entry));
    assertFalse(calendarLit.overlaps(intervalLit).evaluate(adapter, entry));
    assertTrue(dateField.overlaps(intervalLit).evaluate(adapter, entry));
    assertTrue(longLit.overlaps(intervalLit).evaluate(adapter, entry));
    assertTrue(intervalLit.overlaps(dateField).evaluate(adapter, entry));
    assertFalse(instantLit.overlaps(intervalLit).evaluate(adapter, entry));
    assertTrue(
        TemporalLiteral.of(
            Interval.of(Instant.ofEpochMilli(200), Instant.ofEpochMilli(500))).overlaps(
                intervalLit).evaluate(adapter, entry));
    assertFalse(
        TemporalLiteral.of(
            Interval.of(Instant.ofEpochMilli(100), Instant.ofEpochMilli(300))).overlaps(
                intervalLit).evaluate(adapter, entry));
    assertFalse(dateField.overlaps(intervalLit).evaluate(adapter, entryNulls));
    assertFalse(intervalLit.overlaps(dateField).evaluate(adapter, entryNulls));

    // Test serialization
    byte[] bytes = PersistenceUtils.toBinary(dateField.isAfter(longLit));
    final After after = (After) PersistenceUtils.fromBinary(bytes);
    assertTrue(after.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) after.getExpression1()).getFieldName());
    assertTrue(after.getExpression2() instanceof TemporalLiteral);
    assertTrue(((TemporalLiteral) after.getExpression2()).getValue() instanceof Interval);
    assertEquals(
        600,
        ((Interval) ((TemporalLiteral) after.getExpression2()).getValue()).getStart().toEpochMilli());
    assertEquals(
        600,
        ((Interval) ((TemporalLiteral) after.getExpression2()).getValue()).getEnd().toEpochMilli());

    bytes = PersistenceUtils.toBinary(dateField.isDuringOrAfter(intervalLit));
    final DuringOrAfter duringOrAfter = (DuringOrAfter) PersistenceUtils.fromBinary(bytes);
    assertTrue(duringOrAfter.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) duringOrAfter.getExpression1()).getFieldName());
    assertTrue(duringOrAfter.getExpression2() instanceof TemporalLiteral);
    assertTrue(((TemporalLiteral) duringOrAfter.getExpression2()).getValue() instanceof Interval);
    assertEquals(
        450,
        ((Interval) ((TemporalLiteral) duringOrAfter.getExpression2()).getValue()).getStart().toEpochMilli());
    assertEquals(
        650,
        ((Interval) ((TemporalLiteral) duringOrAfter.getExpression2()).getValue()).getEnd().toEpochMilli());

    bytes = PersistenceUtils.toBinary(dateField.isBefore(longLit));
    final Before before = (Before) PersistenceUtils.fromBinary(bytes);
    assertTrue(before.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) before.getExpression1()).getFieldName());
    assertTrue(before.getExpression2() instanceof TemporalLiteral);
    assertTrue(((TemporalLiteral) before.getExpression2()).getValue() instanceof Interval);
    assertEquals(
        600,
        ((Interval) ((TemporalLiteral) before.getExpression2()).getValue()).getStart().toEpochMilli());
    assertEquals(
        600,
        ((Interval) ((TemporalLiteral) before.getExpression2()).getValue()).getEnd().toEpochMilli());

    bytes = PersistenceUtils.toBinary(dateField.isBeforeOrDuring(intervalLit));
    final BeforeOrDuring beforeOrDuring = (BeforeOrDuring) PersistenceUtils.fromBinary(bytes);
    assertTrue(beforeOrDuring.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) beforeOrDuring.getExpression1()).getFieldName());
    assertTrue(beforeOrDuring.getExpression2() instanceof TemporalLiteral);
    assertTrue(((TemporalLiteral) beforeOrDuring.getExpression2()).getValue() instanceof Interval);
    assertEquals(
        450,
        ((Interval) ((TemporalLiteral) beforeOrDuring.getExpression2()).getValue()).getStart().toEpochMilli());
    assertEquals(
        650,
        ((Interval) ((TemporalLiteral) beforeOrDuring.getExpression2()).getValue()).getEnd().toEpochMilli());

    bytes = PersistenceUtils.toBinary(dateField.isDuring(TemporalLiteral.of(null)));
    final During during = (During) PersistenceUtils.fromBinary(bytes);
    assertTrue(during.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) during.getExpression1()).getFieldName());
    assertTrue(during.getExpression2() instanceof TemporalLiteral);
    assertNull(((TemporalLiteral) during.getExpression2()).getValue());

    bytes = PersistenceUtils.toBinary(dateField.isBetween(longLit, intervalLit));
    between = (TemporalBetween) PersistenceUtils.fromBinary(bytes);
    assertTrue(between.getValue() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) between.getValue()).getFieldName());
    assertTrue(between.getLowerBound() instanceof TemporalLiteral);
    assertTrue(((TemporalLiteral) between.getLowerBound()).getValue() instanceof Interval);
    assertEquals(
        600,
        ((Interval) ((TemporalLiteral) between.getLowerBound()).getValue()).getStart().toEpochMilli());
    assertEquals(
        600,
        ((Interval) ((TemporalLiteral) between.getLowerBound()).getValue()).getEnd().toEpochMilli());
    assertTrue(between.getUpperBound() instanceof TemporalLiteral);
    assertTrue(((TemporalLiteral) between.getUpperBound()).getValue() instanceof Interval);
    assertEquals(
        450,
        ((Interval) ((TemporalLiteral) between.getUpperBound()).getValue()).getStart().toEpochMilli());
    assertEquals(
        650,
        ((Interval) ((TemporalLiteral) between.getUpperBound()).getValue()).getEnd().toEpochMilli());

    bytes = PersistenceUtils.toBinary(dateField.overlaps(intervalLit));
    final TimeOverlaps overlaps = (TimeOverlaps) PersistenceUtils.fromBinary(bytes);
    assertTrue(overlaps.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) overlaps.getExpression1()).getFieldName());
    assertTrue(overlaps.getExpression2() instanceof TemporalLiteral);
    assertTrue(((TemporalLiteral) overlaps.getExpression2()).getValue() instanceof Interval);
    assertEquals(
        450,
        ((Interval) ((TemporalLiteral) overlaps.getExpression2()).getValue()).getStart().toEpochMilli());
    assertEquals(
        650,
        ((Interval) ((TemporalLiteral) overlaps.getExpression2()).getValue()).getEnd().toEpochMilli());
  }

  public static class TestType {
    public Geometry geom;
    public Date date;
    public String name;

    public TestType(final Geometry geom, final Date date, final String name) {
      this.geom = geom;
      this.date = date;
      this.name = name;
    }
  }

  public static class TestTypeBasicDataAdapter extends AbstractDataTypeAdapter<TestType> {

    static final FieldDescriptor<?>[] fields =
        new FieldDescriptor<?>[] {
            new SpatialFieldDescriptorBuilder<>(Geometry.class).fieldName("geom").build(),
            new TemporalFieldDescriptorBuilder<>(Date.class).fieldName("date").build(),
            new FieldDescriptorBuilder<>(String.class).fieldName("name").build(),
            new FieldDescriptorBuilder<>(Long.class).fieldName("EMPLOYED").build(),
            new FieldDescriptorBuilder<>(Long.class).fieldName("UNEMPLOY").build(),
            new FieldDescriptorBuilder<>(Boolean.class).fieldName("bool").build(),
            new FieldDescriptorBuilder<>(Integer.class).fieldName("A").build(),
            new FieldDescriptorBuilder<>(Integer.class).fieldName("B").build(),
            new FieldDescriptorBuilder<>(Integer.class).fieldName("C").build(),
            new FieldDescriptorBuilder<>(Integer.class).fieldName("D").build(),
            new FieldDescriptorBuilder<>(Integer.class).fieldName("E").build()};

    public TestTypeBasicDataAdapter() {}

    public TestTypeBasicDataAdapter(final String typeName) {
      super(typeName, fields, fields[2]);
    }

    @Override
    public Object getFieldValue(TestType entry, String fieldName) {
      switch (fieldName) {
        case "geom":
          return entry.geom;
        case "date":
          return entry.date;
        case "name":
          return entry.name;
      }
      return null;
    }

    @Override
    public TestType buildObject(final Object dataId, Object[] fieldValues) {
      return new TestType(
          (Geometry) fieldValues[0],
          (Date) fieldValues[1],
          (String) fieldValues[2]);
    }

  }

}
