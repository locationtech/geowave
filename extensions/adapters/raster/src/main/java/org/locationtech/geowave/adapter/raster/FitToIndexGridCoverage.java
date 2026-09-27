/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster;

import java.awt.image.RenderedImage;
import java.awt.image.renderable.RenderableImage;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.jts.geom.Geometry;
import org.geotools.api.coverage.CannotEvaluateException;
import org.geotools.api.coverage.PointOutsideCoverageException;
import org.geotools.api.coverage.SampleDimension;
import org.geotools.api.coverage.grid.GridCoverage;
import org.geotools.api.coverage.grid.GridGeometry;
import org.geotools.api.geometry.Position;
import org.geotools.api.geometry.Bounds;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.util.Record;
import org.geotools.api.util.RecordType;

public class FitToIndexGridCoverage implements GridCoverage {
  private final GridCoverage gridCoverage;
  private final byte[] partitionKey;
  private final byte[] sortKey;
  private final Resolution resolution;
  private final Bounds originalEnvelope;
  private final Geometry footprintWorldGeometry;
  private final Geometry footprintScreenGeometry;
  private final Map properties;

  public FitToIndexGridCoverage(
      final GridCoverage gridCoverage,
      final byte[] partitionKey,
      final byte[] sortKey,
      final Resolution resolution,
      final Bounds originalEnvelope,
      final Geometry footprintWorldGeometry,
      final Geometry footprintScreenGeometry,
      final Map properties) {
    this.gridCoverage = gridCoverage;
    this.partitionKey = partitionKey;
    this.sortKey = sortKey;
    this.resolution = resolution;
    this.originalEnvelope = originalEnvelope;
    this.footprintWorldGeometry = footprintWorldGeometry;
    this.footprintScreenGeometry = footprintScreenGeometry;
    this.properties = properties;
  }

  public Map getProperties() {
    return properties;
  }

  public Geometry getFootprintWorldGeometry() {
    return footprintWorldGeometry;
  }

  public Geometry getFootprintScreenGeometry() {
    return footprintScreenGeometry;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public byte[] getSortKey() {
    return sortKey;
  }

  public Resolution getResolution() {
    return resolution;
  }

  public GridCoverage getOriginalCoverage() {
    return gridCoverage;
  }

  public Bounds getOriginalEnvelope() {
    return originalEnvelope;
  }

  @Override
  public boolean isDataEditable() {
    return gridCoverage.isDataEditable();
  }

  @Override
  public GridGeometry getGridGeometry() {
    return gridCoverage.getGridGeometry();
  }

  @Override
  public int[] getOptimalDataBlockSizes() {
    return gridCoverage.getOptimalDataBlockSizes();
  }

  @Override
  public int getNumOverviews() {
    return gridCoverage.getNumOverviews();
  }

  @Override
  public GridGeometry getOverviewGridGeometry(final int index) throws IndexOutOfBoundsException {
    return gridCoverage.getOverviewGridGeometry(index);
  }

  @Override
  public GridCoverage getOverview(final int index) throws IndexOutOfBoundsException {
    return gridCoverage.getOverview(index);
  }

  @Override
  public CoordinateReferenceSystem getCoordinateReferenceSystem() {
    return gridCoverage.getCoordinateReferenceSystem();
  }

  @Override
  public Bounds getEnvelope() {
    return gridCoverage.getEnvelope();
  }

  @Override
  public List<GridCoverage> getSources() {
    return gridCoverage.getSources();
  }

  @Override
  public RecordType getRangeType() {
    return gridCoverage.getRangeType();
  }

  @Override
  public Set<Record> evaluate(final Position p, final Collection<String> list)
      throws PointOutsideCoverageException, CannotEvaluateException {
    return gridCoverage.evaluate(p, list);
  }

  @Override
  public RenderedImage getRenderedImage() {
    return gridCoverage.getRenderedImage();
  }

  @Override
  public Object evaluate(final Position point)
      throws PointOutsideCoverageException, CannotEvaluateException {
    return gridCoverage.evaluate(point);
  }

  @Override
  public boolean[] evaluate(final Position point, final boolean[] destination)
      throws PointOutsideCoverageException, CannotEvaluateException,
      ArrayIndexOutOfBoundsException {
    return gridCoverage.evaluate(point, destination);
  }

  @Override
  public byte[] evaluate(final Position point, final byte[] destination)
      throws PointOutsideCoverageException, CannotEvaluateException,
      ArrayIndexOutOfBoundsException {
    return gridCoverage.evaluate(point, destination);
  }

  @Override
  public int[] evaluate(final Position point, final int[] destination)
      throws PointOutsideCoverageException, CannotEvaluateException,
      ArrayIndexOutOfBoundsException {
    return gridCoverage.evaluate(point, destination);
  }

  @Override
  public float[] evaluate(final Position point, final float[] destination)
      throws PointOutsideCoverageException, CannotEvaluateException,
      ArrayIndexOutOfBoundsException {
    return gridCoverage.evaluate(point, destination);
  }

  @Override
  public double[] evaluate(final Position point, final double[] destination)
      throws PointOutsideCoverageException, CannotEvaluateException,
      ArrayIndexOutOfBoundsException {
    return gridCoverage.evaluate(point, destination);
  }

  @Override
  public int getNumSampleDimensions() {
    return gridCoverage.getNumSampleDimensions();
  }

  @Override
  public SampleDimension getSampleDimension(final int index) throws IndexOutOfBoundsException {
    return gridCoverage.getSampleDimension(index);
  }

  @Override
  public RenderableImage getRenderableImage(final int xAxis, final int yAxis)
      throws UnsupportedOperationException, IndexOutOfBoundsException {
    return gridCoverage.getRenderableImage(xAxis, yAxis);
  }
}
