/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.gwql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Date;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveSpatialField;
import org.locationtech.geowave.core.geotime.adapter.annotation.GeoWaveTemporalField;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.BBox;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.BinarySpatialPredicate;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Crosses;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Disjoint;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Intersects;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Overlaps;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialContains;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialEqualTo;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.SpatialLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Touches;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.Within;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.After;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.Before;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.BeforeOrDuring;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.BinaryTemporalPredicate;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.During;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.DuringOrAfter;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalBetween;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalExpression;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalLiteral;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TimeOverlaps;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.gwql.AbstractGWQLTest;
import org.locationtech.geowave.core.store.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.core.store.query.gwql.statement.SelectStatement;
import org.locationtech.geowave.core.store.query.gwql.statement.Statement;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class GWQLParserTest extends AbstractGWQLTest {

  @Override
  protected DataTypeAdapter<?> createDefaultAdapter() {
    return BasicDataTypeAdapter.newAdapter("type", SpatialTemporalType.class, "pid");
  }

  @GeoWaveDataType
  protected static class SpatialTemporalType extends DefaultGWQLTestType {
    @GeoWaveSpatialField
    private Geometry geometry;

    @GeoWaveTemporalField
    private Date start;

    @GeoWaveTemporalField
    private Date end;

    public SpatialTemporalType() {}

    public SpatialTemporalType(
        final String pid,
        final Long pop,
        final String comment,
        final Geometry geometry,
        final Date start,
        final Date end) {
      super(pid, pop, comment);
      this.geometry = geometry;
      this.start = start;
      this.end = end;
    }
  }

  @Test
  public void testTemporalOperatorFunctions() {
    final DataStore dataStore = createDataStore();
    String statement = "SELECT * FROM type WHERE start AFTER '2020-01-01'";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof After);
    BinaryTemporalPredicate predicate = (BinaryTemporalPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof TemporalFieldValue);
    assertEquals("start", ((TemporalFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TemporalLiteral);
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getStart().toEpochMilli());
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getEnd().toEpochMilli());

    statement = "SELECT * FROM type WHERE start DURING_OR_AFTER '2020-01-01/2020-01-05'";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof DuringOrAfter);
    predicate = (BinaryTemporalPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof TemporalFieldValue);
    assertEquals("start", ((TemporalFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TemporalLiteral);
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getStart().toEpochMilli());
    assertEquals(
        TemporalExpression.stringToDate("2020-01-05").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getEnd().toEpochMilli());

    statement = "SELECT * FROM type WHERE start DURING '2020-01-01/2020-01-05'";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof During);
    predicate = (BinaryTemporalPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof TemporalFieldValue);
    assertEquals("start", ((TemporalFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TemporalLiteral);
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getStart().toEpochMilli());
    assertEquals(
        TemporalExpression.stringToDate("2020-01-05").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getEnd().toEpochMilli());

    statement = "SELECT * FROM type WHERE start BEFORE_OR_DURING '2020-01-01/2020-01-05'";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof BeforeOrDuring);
    predicate = (BinaryTemporalPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof TemporalFieldValue);
    assertEquals("start", ((TemporalFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TemporalLiteral);
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getStart().toEpochMilli());
    assertEquals(
        TemporalExpression.stringToDate("2020-01-05").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getEnd().toEpochMilli());

    statement = "SELECT * FROM type WHERE start BEFORE '2020-01-05'";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Before);
    predicate = (BinaryTemporalPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof TemporalFieldValue);
    assertEquals("start", ((TemporalFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TemporalLiteral);
    assertEquals(
        TemporalExpression.stringToDate("2020-01-05").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getStart().toEpochMilli());
    assertEquals(
        TemporalExpression.stringToDate("2020-01-05").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getEnd().toEpochMilli());
  }

  @Test
  public void testSpatialPredicateFunctions()
      throws NoSuchAuthorityCodeException, FactoryException {
    final Geometry point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
    final Geometry bbox =
        GeometryUtils.GEOMETRY_FACTORY.createPolygon(
            new Coordinate[] {
                new Coordinate(0, 0),
                new Coordinate(0, 1),
                new Coordinate(1, 1),
                new Coordinate(1, 0),
                new Coordinate(0, 0)});
    final CoordinateReferenceSystem altCRS = CRS.decode("EPSG:3857");

    final DataStore dataStore = createDataStore();
    String statement = "SELECT * FROM type WHERE intersects(geometry, 'POINT(1 1)')";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof Intersects);
    assertFalse(((Intersects) filter).isLoose());
    BinarySpatialPredicate predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE intersectsLoose(geometry, 'POINT(1 1)')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Intersects);
    assertTrue(((Intersects) filter).isLoose());
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE bbox(geometry, 0, 0, 1, 1)";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof BBox);
    assertFalse(((BBox) filter).isLoose());
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        bbox.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE bboxLoose(geometry, 0, 0, 1, 1)";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof BBox);
    assertTrue(((BBox) filter).isLoose());
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        bbox.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE bbox(geometry, 0, 0, 1, 1, 'EPSG:3857')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof BBox);
    assertFalse(((BBox) filter).isLoose());
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        bbox.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(altCRS, predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE bboxLoose(geometry, 0, 0, 1, 1, 'EPSG:3857')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof BBox);
    assertTrue(((BBox) filter).isLoose());
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        bbox.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(altCRS, predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE disjoint(geometry, 'POINT(1 1)')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Disjoint);
    assertFalse(((Disjoint) filter).isLoose());
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE disjointLoose(geometry, 'POINT(1 1)')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Disjoint);
    assertTrue(((Disjoint) filter).isLoose());
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE crosses(geometry, 'POINT(1 1)')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Crosses);
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE touches(geometry, 'POINT(1 1)')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Touches);
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE overlaps(geometry, 'POINT(1 1)')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Overlaps);
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE contains(geometry, 'POINT(1 1)')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof SpatialContains);
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));

    statement = "SELECT * FROM type WHERE within(geometry, 'POINT(1 1)')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Within);
    predicate = (BinarySpatialPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof SpatialLiteral);
    assertTrue(
        point.equalsExact(((SpatialLiteral) predicate.getExpression2()).getValue().getGeometry()));
    assertEquals(GeometryUtils.getDefaultCRS(), predicate.getExpression2().getCRS(null));
  }

  @Test
  public void testTemporalPredicateFunctions() {
    final DataStore dataStore = createDataStore();
    String statement = "SELECT * FROM type WHERE tcontains(start, '2020-01-01')";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    // During is the inverse of contains, so the operands should be flipped
    assertTrue(filter instanceof During);
    BinaryTemporalPredicate predicate = (BinaryTemporalPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof TemporalLiteral);
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression1()).getValue().getStart().toEpochMilli());
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression1()).getValue().getEnd().toEpochMilli());
    assertTrue(predicate.getExpression2() instanceof TemporalFieldValue);
    assertEquals("start", ((TemporalFieldValue) predicate.getExpression2()).getFieldName());

    statement = "SELECT * FROM type WHERE toverlaps(start, '2020-01-01')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof TimeOverlaps);
    predicate = (BinaryTemporalPredicate) filter;
    assertTrue(predicate.getExpression1() instanceof TemporalFieldValue);
    assertEquals("start", ((TemporalFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TemporalLiteral);
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getStart().toEpochMilli());
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) predicate.getExpression2()).getValue().getEnd().toEpochMilli());
  }

  @Test
  public void testCasting() {
    final DataStore dataStore = createDataStore();
    String statement = "SELECT * FROM type WHERE pop::date BETWEEN '2020-01-01' AND '2020-01-02'";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof TemporalBetween);
    final TemporalBetween between = (TemporalBetween) filter;
    assertTrue(between.getValue() instanceof TemporalFieldValue);
    assertEquals("pop", ((TemporalFieldValue) between.getValue()).getFieldName());
    assertTrue(between.getLowerBound() instanceof TemporalLiteral);
    assertTrue(between.getUpperBound() instanceof TemporalLiteral);

    statement = "SELECT * FROM type WHERE geometry = 'POINT(1 1)'::geometry";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof SpatialEqualTo);
    final SpatialEqualTo equals = (SpatialEqualTo) filter;
    assertTrue(equals.getExpression1() instanceof SpatialFieldValue);
    assertEquals("geometry", ((SpatialFieldValue) equals.getExpression1()).getFieldName());
    assertTrue(equals.getExpression2() instanceof SpatialLiteral);
  }
}
