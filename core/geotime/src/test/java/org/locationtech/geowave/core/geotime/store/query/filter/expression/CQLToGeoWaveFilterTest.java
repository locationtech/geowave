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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.text.ParseException;
import java.time.Instant;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.BBox;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Crosses;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Disjoint;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Intersects;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Overlaps;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialContains;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialLiteral;
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
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.query.filter.expression.And;
import org.locationtech.geowave.core.store.query.filter.expression.BooleanFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.BooleanLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator.CompareOp;
import org.locationtech.geowave.core.store.query.filter.expression.Exclude;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.FilterConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.GenericEqualTo;
import org.locationtech.geowave.core.store.query.filter.expression.Include;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.IndexFieldConstraints.DimensionConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.IsNull;
import org.locationtech.geowave.core.store.query.filter.expression.Not;
import org.locationtech.geowave.core.store.query.filter.expression.Or;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Add;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Divide;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Multiply;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericBetween;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Subtract;
import org.locationtech.geowave.core.store.query.filter.expression.text.Contains;
import org.locationtech.geowave.core.store.query.filter.expression.text.EndsWith;
import org.locationtech.geowave.core.store.query.filter.expression.text.StartsWith;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.threeten.extra.Interval;
import com.google.common.collect.Sets;

public class CQLToGeoWaveFilterTest {

  private static final double EPSILON = 0.0000001;
  private static DataTypeAdapter<?> adapter =
      new SpatialTemporalFilterExpressionTest.TestTypeBasicDataAdapter("test");

  @Test
  public void testCQLtoGeoWaveFilter() throws CQLException, ParseException {
    Filter f = fromCQL("EMPLOYED < 15000000");
    assertTrue(f instanceof NumericComparisonOperator);
    assertEquals(CompareOp.LESS_THAN, ((NumericComparisonOperator) f).getCompareOp());
    assertTrue(((NumericComparisonOperator) f).getExpression1() instanceof NumericFieldValue);
    assertEquals(
        "EMPLOYED",
        ((NumericFieldValue) ((NumericComparisonOperator) f).getExpression1()).getFieldName());
    assertTrue(((NumericComparisonOperator) f).getExpression2() instanceof NumericLiteral);
    assertEquals(
        15000000L,
        ((NumericLiteral) ((NumericComparisonOperator) f).getExpression2()).getValue().longValue());

    f = fromCQL("EMPLOYED BETWEEN 1000000 AND 3000000");
    assertTrue(f instanceof NumericBetween);
    assertTrue(((NumericBetween) f).getValue() instanceof NumericFieldValue);
    assertEquals("EMPLOYED", ((NumericFieldValue) ((NumericBetween) f).getValue()).getFieldName());
    assertTrue(((NumericBetween) f).getLowerBound() instanceof NumericLiteral);
    assertEquals(
        1000000L,
        ((NumericLiteral) ((NumericBetween) f).getLowerBound()).getValue().longValue());
    assertTrue(((NumericBetween) f).getUpperBound() instanceof NumericLiteral);
    assertEquals(
        3000000L,
        ((NumericLiteral) ((NumericBetween) f).getUpperBound()).getValue().longValue());

    f = fromCQL("name = 'California'");
    assertTrue(f instanceof TextComparisonOperator);
    assertEquals(CompareOp.EQUAL_TO, ((TextComparisonOperator) f).getCompareOp());
    assertTrue(((TextComparisonOperator) f).getExpression1() instanceof TextFieldValue);
    assertEquals(
        "name",
        ((TextFieldValue) ((TextComparisonOperator) f).getExpression1()).getFieldName());
    assertTrue(((TextComparisonOperator) f).getExpression2() instanceof TextLiteral);
    assertEquals(
        "California",
        ((TextLiteral) ((TextComparisonOperator) f).getExpression2()).getValue());

    f = fromCQL("UNEMPLOY / (EMPLOYED + UNEMPLOY) > 0.07");
    assertTrue(f instanceof NumericComparisonOperator);
    assertEquals(CompareOp.GREATER_THAN, ((NumericComparisonOperator) f).getCompareOp());
    assertTrue(((NumericComparisonOperator) f).getExpression1() instanceof Divide);
    Divide divide = (Divide) ((NumericComparisonOperator) f).getExpression1();
    assertTrue(divide.getExpression1() instanceof NumericFieldValue);
    assertEquals("UNEMPLOY", ((NumericFieldValue) divide.getExpression1()).getFieldName());
    assertTrue(divide.getExpression2() instanceof Add);
    Add add = (Add) divide.getExpression2();
    assertTrue(add.getExpression1() instanceof NumericFieldValue);
    assertEquals("EMPLOYED", ((NumericFieldValue) add.getExpression1()).getFieldName());
    assertTrue(add.getExpression2() instanceof NumericFieldValue);
    assertEquals("UNEMPLOY", ((NumericFieldValue) add.getExpression2()).getFieldName());
    assertTrue(((NumericComparisonOperator) f).getExpression2() instanceof NumericLiteral);
    assertEquals(
        0.07,
        ((NumericLiteral) ((NumericComparisonOperator) f).getExpression2()).getValue(),
        EPSILON);

    f = fromCQL("A <> B AND B <= 8.1");
    assertTrue(f instanceof And);
    assertTrue(((And) f).getChildren().length == 2);
    assertTrue(((And) f).getChildren()[0] instanceof Not);
    assertTrue(((Not) ((And) f).getChildren()[0]).getFilter() instanceof NumericComparisonOperator);
    NumericComparisonOperator equalTo =
        (NumericComparisonOperator) ((Not) ((And) f).getChildren()[0]).getFilter();
    assertEquals(CompareOp.EQUAL_TO, equalTo.getCompareOp());
    assertTrue(equalTo.getExpression1() instanceof NumericFieldValue);
    assertEquals("A", ((NumericFieldValue) equalTo.getExpression1()).getFieldName());
    assertTrue(equalTo.getExpression2() instanceof NumericFieldValue);
    assertEquals("B", ((NumericFieldValue) equalTo.getExpression2()).getFieldName());
    assertTrue(((And) f).getChildren()[1] instanceof NumericComparisonOperator);
    NumericComparisonOperator lessThan = (NumericComparisonOperator) ((And) f).getChildren()[1];
    assertEquals(CompareOp.LESS_THAN_OR_EQUAL, lessThan.getCompareOp());
    assertTrue(lessThan.getExpression1() instanceof NumericFieldValue);
    assertEquals("B", ((NumericFieldValue) lessThan.getExpression1()).getFieldName());
    assertTrue(lessThan.getExpression2() instanceof NumericLiteral);
    assertEquals(8.1, ((NumericLiteral) lessThan.getExpression2()).getValue(), EPSILON);

    // Order of operations should be preserved
    f = fromCQL("A + B - (C * D) / 8.5 >= E");
    assertTrue(f instanceof NumericComparisonOperator);
    assertEquals(CompareOp.GREATER_THAN_OR_EQUAL, ((NumericComparisonOperator) f).getCompareOp());
    assertTrue(((NumericComparisonOperator) f).getExpression1() instanceof Subtract);
    Subtract subtract = (Subtract) ((NumericComparisonOperator) f).getExpression1();
    assertTrue(subtract.getExpression1() instanceof Add);
    add = (Add) subtract.getExpression1();
    assertTrue(add.getExpression1() instanceof NumericFieldValue);
    assertEquals("A", ((NumericFieldValue) add.getExpression1()).getFieldName());
    assertTrue(add.getExpression2() instanceof NumericFieldValue);
    assertEquals("B", ((NumericFieldValue) add.getExpression2()).getFieldName());
    assertTrue(subtract.getExpression2() instanceof Divide);
    divide = (Divide) subtract.getExpression2();
    assertTrue(divide.getExpression1() instanceof Multiply);
    Multiply multiply = (Multiply) divide.getExpression1();
    assertTrue(multiply.getExpression1() instanceof NumericFieldValue);
    assertEquals("C", ((NumericFieldValue) multiply.getExpression1()).getFieldName());
    assertTrue(multiply.getExpression2() instanceof NumericFieldValue);
    assertEquals("D", ((NumericFieldValue) multiply.getExpression2()).getFieldName());
    assertTrue(divide.getExpression2() instanceof NumericLiteral);
    assertEquals(8.5, ((NumericLiteral) divide.getExpression2()).getValue(), EPSILON);
    assertTrue(((NumericComparisonOperator) f).getExpression2() instanceof NumericFieldValue);
    assertEquals(
        "E",
        ((NumericFieldValue) ((NumericComparisonOperator) f).getExpression2()).getFieldName());

    f = fromCQL("BBOX(geom, -90, 40, -60, 45)");
    assertTrue(f instanceof BBox);
    assertTrue(((BBox) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) ((BBox) f).getExpression1()).getFieldName());
    assertTrue(((BBox) f).getExpression2() instanceof SpatialLiteral);
    SpatialLiteral spatialLit = (SpatialLiteral) ((BBox) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    Geometry geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(-90, -60, 40, 45))));

    f = fromCQL("DISJOINT(geom, POLYGON((-90 40, -90 45, -60 45, -60 40, -90 40)))");
    assertTrue(f instanceof Disjoint);
    assertTrue(((Disjoint) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) ((Disjoint) f).getExpression1()).getFieldName());
    assertTrue(((Disjoint) f).getExpression2() instanceof SpatialLiteral);
    spatialLit = (SpatialLiteral) ((Disjoint) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(-90, 40),
                    new Coordinate(-90, 45),
                    new Coordinate(-60, 45),
                    new Coordinate(-60, 40),
                    new Coordinate(-90, 40)})));

    f = fromCQL("EQUALS(geom, POLYGON((-90 40, -90 45, -60 45, -60 40, -90 40)))");
    assertTrue(f instanceof SpatialEqualTo);
    assertTrue(((SpatialEqualTo) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals(
        "geom",
        ((SpatialFieldValue) ((SpatialEqualTo) f).getExpression1()).getFieldName());
    assertTrue(((SpatialEqualTo) f).getExpression2() instanceof SpatialLiteral);
    spatialLit = (SpatialLiteral) ((SpatialEqualTo) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(-90, 40),
                    new Coordinate(-90, 45),
                    new Coordinate(-60, 45),
                    new Coordinate(-60, 40),
                    new Coordinate(-90, 40)})));

    f = fromCQL("CONTAINS(geom, POLYGON((-90 40, -90 45, -60 45, -60 40, -90 40)))");
    assertTrue(f instanceof SpatialContains);
    assertTrue(((SpatialContains) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals(
        "geom",
        ((SpatialFieldValue) ((SpatialContains) f).getExpression1()).getFieldName());
    assertTrue(((SpatialContains) f).getExpression2() instanceof SpatialLiteral);
    spatialLit = (SpatialLiteral) ((SpatialContains) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(-90, 40),
                    new Coordinate(-90, 45),
                    new Coordinate(-60, 45),
                    new Coordinate(-60, 40),
                    new Coordinate(-90, 40)})));

    f = fromCQL("CROSSES(geom, POLYGON((-90 40, -90 45, -60 45, -60 40, -90 40)))");
    assertTrue(f instanceof Crosses);
    assertTrue(((Crosses) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) ((Crosses) f).getExpression1()).getFieldName());
    assertTrue(((Crosses) f).getExpression2() instanceof SpatialLiteral);
    spatialLit = (SpatialLiteral) ((Crosses) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(-90, 40),
                    new Coordinate(-90, 45),
                    new Coordinate(-60, 45),
                    new Coordinate(-60, 40),
                    new Coordinate(-90, 40)})));

    f = fromCQL("INTERSECTS(geom, POLYGON((-90 40, -90 45, -60 45, -60 40, -90 40)))");
    assertTrue(f instanceof Intersects);
    assertTrue(((Intersects) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) ((Intersects) f).getExpression1()).getFieldName());
    assertTrue(((Intersects) f).getExpression2() instanceof SpatialLiteral);
    spatialLit = (SpatialLiteral) ((Intersects) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(-90, 40),
                    new Coordinate(-90, 45),
                    new Coordinate(-60, 45),
                    new Coordinate(-60, 40),
                    new Coordinate(-90, 40)})));

    f = fromCQL("OVERLAPS(geom, POLYGON((-90 40, -90 45, -60 45, -60 40, -90 40)))");
    assertTrue(f instanceof Overlaps);
    assertTrue(((Overlaps) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) ((Overlaps) f).getExpression1()).getFieldName());
    assertTrue(((Overlaps) f).getExpression2() instanceof SpatialLiteral);
    spatialLit = (SpatialLiteral) ((Overlaps) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(-90, 40),
                    new Coordinate(-90, 45),
                    new Coordinate(-60, 45),
                    new Coordinate(-60, 40),
                    new Coordinate(-90, 40)})));

    f = fromCQL("TOUCHES(geom, POLYGON((-90 40, -90 45, -60 45, -60 40, -90 40)))");
    assertTrue(f instanceof Touches);
    assertTrue(((Touches) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) ((Touches) f).getExpression1()).getFieldName());
    assertTrue(((Touches) f).getExpression2() instanceof SpatialLiteral);
    spatialLit = (SpatialLiteral) ((Touches) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(-90, 40),
                    new Coordinate(-90, 45),
                    new Coordinate(-60, 45),
                    new Coordinate(-60, 40),
                    new Coordinate(-90, 40)})));

    f = fromCQL("WITHIN(geom, POLYGON((-90 40, -90 45, -60 45, -60 40, -90 40)))");
    assertTrue(f instanceof Within);
    assertTrue(((Within) f).getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) ((Within) f).getExpression1()).getFieldName());
    assertTrue(((Within) f).getExpression2() instanceof SpatialLiteral);
    spatialLit = (SpatialLiteral) ((Within) f).getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(
            GeometryUtils.GEOMETRY_FACTORY.createPolygon(
                new Coordinate[] {
                    new Coordinate(-90, 40),
                    new Coordinate(-90, 45),
                    new Coordinate(-60, 45),
                    new Coordinate(-60, 40),
                    new Coordinate(-90, 40)})));

    final Instant date1 = Instant.parse("2020-01-25T00:28:32Z");
    final Instant date2 = Instant.parse("2021-03-02T13:08:45Z");

    f = fromCQL("date AFTER 2020-01-25T00:28:32Z");
    assertTrue(f instanceof After);
    assertTrue(((After) f).getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) ((After) f).getExpression1()).getFieldName());
    assertTrue(((After) f).getExpression2() instanceof TemporalLiteral);
    Interval interval = ((TemporalLiteral) ((After) f).getExpression2()).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date1.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date > 2020-01-25T00:28:32Z");
    assertTrue(f instanceof After);
    assertTrue(((After) f).getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) ((After) f).getExpression1()).getFieldName());
    assertTrue(((After) f).getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) ((After) f).getExpression2()).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date1.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date BEFORE 2021-03-02T13:08:45Z");
    assertTrue(f instanceof Before);
    assertTrue(((Before) f).getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) ((Before) f).getExpression1()).getFieldName());
    assertTrue(((Before) f).getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) ((Before) f).getExpression2()).getValue();
    assertEquals(date2.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date2.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date < 2021-03-02T13:08:45Z");
    assertTrue(f instanceof Before);
    assertTrue(((Before) f).getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) ((Before) f).getExpression1()).getFieldName());
    assertTrue(((Before) f).getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) ((Before) f).getExpression2()).getValue();
    assertEquals(date2.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date2.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date DURING 2020-01-25T00:28:32Z/2021-03-02T13:08:45Z");
    assertTrue(f instanceof During);
    assertTrue(((During) f).getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) ((During) f).getExpression1()).getFieldName());
    assertTrue(((During) f).getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) ((During) f).getExpression2()).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date2.getEpochSecond(), interval.getEnd().getEpochSecond());

    // GeoWave has a BeforeOrDuring class, but the CQL filter translates it using OR
    f = fromCQL("date BEFORE OR DURING 2020-01-25T00:28:32Z/2021-03-02T13:08:45Z");
    assertTrue(f instanceof Or);
    assertTrue(((Or) f).getChildren().length == 2);
    assertTrue(((Or) f).getChildren()[0] instanceof Before);
    Before before = (Before) ((Or) f).getChildren()[0];
    assertTrue(before.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) before.getExpression1()).getFieldName());
    assertTrue(before.getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) before.getExpression2()).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date1.getEpochSecond(), interval.getEnd().getEpochSecond());

    assertTrue(((Or) f).getChildren()[1] instanceof During);
    During during = (During) ((Or) f).getChildren()[1];
    assertTrue(during.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) during.getExpression1()).getFieldName());
    assertTrue(during.getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) during.getExpression2()).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date2.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date DURING OR AFTER 2020-01-25T00:28:32Z/2021-03-02T13:08:45Z");
    assertTrue(f instanceof Or);
    assertTrue(((Or) f).getChildren().length == 2);
    assertTrue(((Or) f).getChildren()[0] instanceof During);
    during = (During) ((Or) f).getChildren()[0];
    assertTrue(during.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) during.getExpression1()).getFieldName());
    assertTrue(during.getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) during.getExpression2()).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date2.getEpochSecond(), interval.getEnd().getEpochSecond());

    assertTrue(((Or) f).getChildren()[1] instanceof After);
    After after = (After) ((Or) f).getChildren()[1];
    assertTrue(after.getExpression1() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) after.getExpression1()).getFieldName());
    assertTrue(after.getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) after.getExpression2()).getValue();
    assertEquals(date2.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date2.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date <= 2020-01-25T00:28:32Z");
    assertTrue(f instanceof BeforeOrDuring);
    assertTrue(((BeforeOrDuring) f).getExpression1() instanceof TemporalFieldValue);
    assertEquals(
        "date",
        ((TemporalFieldValue) ((BeforeOrDuring) f).getExpression1()).getFieldName());
    assertTrue(((BeforeOrDuring) f).getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) ((BeforeOrDuring) f).getExpression2()).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date1.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date >= 2020-01-25T00:28:32Z");
    assertTrue(f instanceof DuringOrAfter);
    assertTrue(((DuringOrAfter) f).getExpression1() instanceof TemporalFieldValue);
    assertEquals(
        "date",
        ((TemporalFieldValue) ((DuringOrAfter) f).getExpression1()).getFieldName());
    assertTrue(((DuringOrAfter) f).getExpression2() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) ((DuringOrAfter) f).getExpression2()).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date1.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date BETWEEN 2020-01-25T00:28:32Z AND 2021-03-02T13:08:45Z");
    assertTrue(f instanceof TemporalBetween);
    assertTrue(((TemporalBetween) f).getValue() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) ((TemporalBetween) f).getValue()).getFieldName());
    assertTrue(((TemporalBetween) f).getLowerBound() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) (((TemporalBetween) f).getLowerBound())).getValue();
    assertEquals(date1.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date1.getEpochSecond(), interval.getEnd().getEpochSecond());
    assertTrue(((TemporalBetween) f).getUpperBound() instanceof TemporalLiteral);
    interval = ((TemporalLiteral) (((TemporalBetween) f).getUpperBound())).getValue();
    assertEquals(date2.getEpochSecond(), interval.getStart().getEpochSecond());
    assertEquals(date2.getEpochSecond(), interval.getEnd().getEpochSecond());

    f = fromCQL("date IS NULL");
    assertTrue(f instanceof IsNull);
    assertTrue(((IsNull) f).getExpression() instanceof TemporalFieldValue);
    assertEquals("date", ((TemporalFieldValue) ((IsNull) f).getExpression()).getFieldName());

    f = fromCQL("date IS NOT NULL");
    assertTrue(f instanceof Not);
    assertTrue(((Not) f).getFilter() instanceof IsNull);
    assertTrue(((IsNull) ((Not) f).getFilter()).getExpression() instanceof TemporalFieldValue);
    assertEquals(
        "date",
        ((TemporalFieldValue) ((IsNull) ((Not) f).getFilter()).getExpression()).getFieldName());

    f = fromCQL("INCLUDE");
    assertTrue(f instanceof Include);

    f = fromCQL("EXCLUDE");
    assertTrue(f instanceof Exclude);

    f = fromCQL("bool = TRUE");
    assertTrue(f instanceof GenericEqualTo);
    assertTrue(((GenericEqualTo) f).getExpression1() instanceof BooleanFieldValue);
    assertEquals(
        "bool",
        ((BooleanFieldValue) ((GenericEqualTo) f).getExpression1()).getFieldName());
    assertTrue(((GenericEqualTo) f).getExpression2() instanceof BooleanLiteral);
    assertTrue((boolean) ((BooleanLiteral) ((GenericEqualTo) f).getExpression2()).getValue());

    f = fromCQL("name LIKE '%value'");
    assertTrue(f instanceof EndsWith);
    assertTrue(((EndsWith) f).getExpression1() instanceof TextFieldValue);
    assertEquals("name", ((TextFieldValue) ((EndsWith) f).getExpression1()).getFieldName());
    assertTrue(((EndsWith) f).getExpression2() instanceof TextLiteral);
    assertEquals("value", ((TextLiteral) ((EndsWith) f).getExpression2()).getValue());

    f = fromCQL("name LIKE 'value%'");
    assertTrue(f instanceof StartsWith);
    assertTrue(((StartsWith) f).getExpression1() instanceof TextFieldValue);
    assertEquals("name", ((TextFieldValue) ((StartsWith) f).getExpression1()).getFieldName());
    assertTrue(((StartsWith) f).getExpression2() instanceof TextLiteral);
    assertEquals("value", ((TextLiteral) ((StartsWith) f).getExpression2()).getValue());

    f = fromCQL("name LIKE '%value%'");
    assertTrue(f instanceof Contains);
    assertTrue(((Contains) f).getExpression1() instanceof TextFieldValue);
    assertEquals("name", ((TextFieldValue) ((Contains) f).getExpression1()).getFieldName());
    assertTrue(((Contains) f).getExpression2() instanceof TextLiteral);
    assertEquals("value", ((TextLiteral) ((Contains) f).getExpression2()).getValue());

    f = fromCQL("name LIKE 'a\\_value'");
    assertTrue(f instanceof TextComparisonOperator);
    assertEquals(CompareOp.EQUAL_TO, ((TextComparisonOperator) f).getCompareOp());
    assertTrue(((TextComparisonOperator) f).getExpression1() instanceof TextFieldValue);
    assertEquals(
        "name",
        ((TextFieldValue) ((TextComparisonOperator) f).getExpression1()).getFieldName());
    assertTrue(((TextComparisonOperator) f).getExpression2() instanceof TextLiteral);
    assertEquals(
        "a_value",
        ((TextLiteral) ((TextComparisonOperator) f).getExpression2()).getValue());

    try {
      // _ is a single character wild card, so this is not supported
      f = fromCQL("name LIKE 'a_value'");
      fail();
    } catch (CQLToGeoWaveConversionException e) {
      // expected
    }

  }

  @Test
  public void testComplexConstraints() throws CQLException {
    final Filter f =
        fromCQL(
            "BBOX(geom, 5, 20, 8, 30) AND ((A BETWEEN 5 AND 10 AND B < 10) OR (A BETWEEN 15 AND 20 AND B > 5)) AND name LIKE 'aBc%'");
    // This filter should result in the following constraints:
    // A -> [5, 10], [15, 20]
    // B -> [null, null] // B > 5 OR B < 10 is a full scan
    // geom -> [5, 8] // geom dimension 0
    // [20, 30] // geom dimension 1
    // str -> ["aBc", "aBd") // "aBd" is exclusive

    assertTrue(f instanceof And);
    assertEquals(2, ((And) f).getChildren().length);
    assertTrue(((And) f).getChildren()[0] instanceof And);
    assertEquals(2, ((And) ((And) f).getChildren()[0]).getChildren().length);
    assertTrue(((And) ((And) f).getChildren()[0]).getChildren()[0] instanceof BBox);
    final BBox bbox = (BBox) ((And) ((And) f).getChildren()[0]).getChildren()[0];
    assertTrue(bbox.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geom", ((SpatialFieldValue) bbox.getExpression1()).getFieldName());
    assertTrue(bbox.getExpression2() instanceof SpatialLiteral);
    SpatialLiteral spatialLit = (SpatialLiteral) bbox.getExpression2();
    assertTrue(spatialLit.getValue() instanceof UnpreparedFilterGeometry);
    Geometry geom = ((UnpreparedFilterGeometry) spatialLit.getValue()).getGeometry();
    assertTrue(
        geom.equalsTopo(GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(5, 8, 20, 30))));
    assertTrue(((And) ((And) f).getChildren()[0]).getChildren()[1] instanceof Or);
    final Or or = (Or) ((And) ((And) f).getChildren()[0]).getChildren()[1];
    assertEquals(2, or.getChildren().length);
    assertTrue(or.getChildren()[0] instanceof And);
    And and = (And) or.getChildren()[0];
    assertEquals(2, and.getChildren().length);
    assertTrue(and.getChildren()[0] instanceof NumericBetween);
    NumericBetween between = (NumericBetween) and.getChildren()[0];
    assertTrue(between.getValue() instanceof NumericFieldValue);
    assertEquals("A", ((NumericFieldValue) between.getValue()).getFieldName());
    assertTrue(between.getLowerBound() instanceof NumericLiteral);
    assertEquals(5L, ((NumericLiteral) between.getLowerBound()).getValue().longValue());
    assertTrue(between.getUpperBound() instanceof NumericLiteral);
    assertEquals(10L, ((NumericLiteral) between.getUpperBound()).getValue().longValue());
    assertTrue(and.getChildren()[1] instanceof NumericComparisonOperator);
    NumericComparisonOperator compareOp = (NumericComparisonOperator) and.getChildren()[1];
    assertEquals(CompareOp.LESS_THAN, compareOp.getCompareOp());
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("B", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(10L, ((NumericLiteral) compareOp.getExpression2()).getValue().longValue());
    assertTrue(or.getChildren()[1] instanceof And);
    and = (And) or.getChildren()[1];
    assertEquals(2, and.getChildren().length);
    assertTrue(and.getChildren()[0] instanceof NumericBetween);
    between = (NumericBetween) and.getChildren()[0];
    assertTrue(between.getValue() instanceof NumericFieldValue);
    assertEquals("A", ((NumericFieldValue) between.getValue()).getFieldName());
    assertTrue(between.getLowerBound() instanceof NumericLiteral);
    assertEquals(15L, ((NumericLiteral) between.getLowerBound()).getValue().longValue());
    assertTrue(between.getUpperBound() instanceof NumericLiteral);
    assertEquals(20L, ((NumericLiteral) between.getUpperBound()).getValue().longValue());
    assertTrue(and.getChildren()[1] instanceof NumericComparisonOperator);
    compareOp = (NumericComparisonOperator) and.getChildren()[1];
    assertEquals(CompareOp.GREATER_THAN, compareOp.getCompareOp());
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("B", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(5L, ((NumericLiteral) compareOp.getExpression2()).getValue().longValue());
    assertTrue(((And) f).getChildren()[1] instanceof StartsWith);
    final StartsWith startsWith = (StartsWith) ((And) f).getChildren()[1];
    assertTrue(startsWith.getExpression1() instanceof TextFieldValue);
    assertEquals("name", ((TextFieldValue) startsWith.getExpression1()).getFieldName());
    assertTrue(startsWith.getExpression2() instanceof TextLiteral);
    assertEquals("aBc", ((TextLiteral) startsWith.getExpression2()).getValue());

    // Check geom constraints
    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    AdapterToIndexMapping mapping =
        BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) 0), spatialIndex);
    FilterConstraints<Double> constraints =
        f.getConstraints(
            Double.class,
            null,
            adapter,
            mapping,
            spatialIndex,
            Sets.newHashSet("geom"));
    IndexFieldConstraints<?> fieldConstraints = constraints.getFieldConstraints("geom");
    assertNotNull(fieldConstraints);
    DimensionConstraints<?> dimRanges = fieldConstraints.getDimensionRanges(0);
    assertNotNull(dimRanges);
    assertEquals(1, dimRanges.getRanges().size());
    assertEquals(5L, ((Double) dimRanges.getRanges().get(0).getStart()).longValue());
    assertTrue(dimRanges.getRanges().get(0).isStartInclusive());
    assertEquals(8L, ((Double) dimRanges.getRanges().get(0).getEnd()).longValue());
    assertTrue(dimRanges.getRanges().get(0).isEndInclusive());
    assertFalse(dimRanges.getRanges().get(0).isExact());
    dimRanges = fieldConstraints.getDimensionRanges(1);
    assertNotNull(dimRanges);
    assertEquals(1, dimRanges.getRanges().size());
    assertEquals(20L, ((Double) dimRanges.getRanges().get(0).getStart()).longValue());
    assertTrue(dimRanges.getRanges().get(0).isStartInclusive());
    assertEquals(30L, ((Double) dimRanges.getRanges().get(0).getEnd()).longValue());
    assertTrue(dimRanges.getRanges().get(0).isEndInclusive());
    assertFalse(dimRanges.getRanges().get(0).isExact());

    // Check A constraints
    final Index aIndex =
        AttributeDimensionalityTypeProvider.createIndexForDescriptor(
            adapter,
            adapter.getFieldDescriptor("A"),
            "aIndex");
    mapping = BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) 0), aIndex);
    constraints =
        f.getConstraints(Double.class, null, adapter, mapping, aIndex, Sets.newHashSet("A"));
    fieldConstraints = constraints.getFieldConstraints("A");
    assertNotNull(fieldConstraints);
    dimRanges = fieldConstraints.getDimensionRanges(0);
    assertNotNull(dimRanges);
    assertEquals(2, dimRanges.getRanges().size());
    assertEquals(5L, ((Double) dimRanges.getRanges().get(0).getStart()).longValue());
    assertTrue(dimRanges.getRanges().get(0).isStartInclusive());
    assertEquals(10L, ((Double) dimRanges.getRanges().get(0).getEnd()).longValue());
    assertTrue(dimRanges.getRanges().get(0).isEndInclusive());
    assertTrue(dimRanges.getRanges().get(0).isExact());
    assertEquals(15L, ((Double) dimRanges.getRanges().get(1).getStart()).longValue());
    assertTrue(dimRanges.getRanges().get(1).isStartInclusive());
    assertEquals(20L, ((Double) dimRanges.getRanges().get(1).getEnd()).longValue());
    assertTrue(dimRanges.getRanges().get(1).isEndInclusive());
    assertTrue(dimRanges.getRanges().get(1).isExact());

    // Check B constraints
    final Index bIndex =
        AttributeDimensionalityTypeProvider.createIndexForDescriptor(
            adapter,
            adapter.getFieldDescriptor("B"),
            "bIndex");
    mapping = BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) 0), bIndex);
    constraints =
        f.getConstraints(Double.class, null, adapter, mapping, bIndex, Sets.newHashSet("B"));
    fieldConstraints = constraints.getFieldConstraints("B");
    assertNotNull(fieldConstraints);
    dimRanges = fieldConstraints.getDimensionRanges(0);
    assertNotNull(dimRanges);
    assertEquals(1, dimRanges.getRanges().size());
    assertNull(dimRanges.getRanges().get(0).getStart());
    assertTrue(dimRanges.getRanges().get(0).isStartInclusive());
    assertNull(dimRanges.getRanges().get(0).getEnd());
    assertTrue(dimRanges.getRanges().get(0).isEndInclusive());
    assertTrue(dimRanges.getRanges().get(0).isExact());

    // Check name constraints
    final Index nameIndex =
        AttributeDimensionalityTypeProvider.createIndexForDescriptor(
            adapter,
            adapter.getFieldDescriptor("name"),
            "nameIndex");
    mapping = BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) 0), nameIndex);
    FilterConstraints<String> textConstraints =
        f.getConstraints(String.class, null, adapter, mapping, nameIndex, Sets.newHashSet("name"));
    fieldConstraints = textConstraints.getFieldConstraints("name");
    assertNotNull(fieldConstraints);
    dimRanges = fieldConstraints.getDimensionRanges(0);
    assertNotNull(dimRanges);
    assertEquals(1, dimRanges.getRanges().size());
    assertEquals("aBc", dimRanges.getRanges().get(0).getStart());
    assertTrue(dimRanges.getRanges().get(0).isStartInclusive());
    assertEquals("aBc", dimRanges.getRanges().get(0).getEnd());
    assertTrue(dimRanges.getRanges().get(0).isEndInclusive());
    assertTrue(dimRanges.getRanges().get(0).isExact());
  }

  private Filter fromCQL(final String cqlStr) throws CQLException {
    final org.opengis.filter.Filter cqlFilter = ECQL.toFilter(cqlStr);
    return (Filter) cqlFilter.accept(new CQLToGeoWaveFilterVisitor(adapter), null);
  }

}
