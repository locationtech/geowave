/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.adapter.raster;

import java.awt.image.RenderedImage;
import java.awt.image.renderable.RenderableImage;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opengis.coverage.CannotEvaluateException;
import org.opengis.coverage.PointOutsideCoverageException;
import org.opengis.coverage.SampleDimension;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridGeometry;
import org.opengis.geometry.DirectPosition;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.Record;
import org.opengis.util.RecordType;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class FitToIndexGridCoverage implements
		GridCoverage
{
	private final GridCoverage gridCoverage;
	private final ByteArrayId insertionId;
	private final Resolution resolution;
	private final Envelope originalEnvelope;
	private final Geometry footprintWorldGeometry;
	private final Geometry footprintScreenGeometry;
	private final Map properties;

	public FitToIndexGridCoverage(
			final GridCoverage gridCoverage,
			final ByteArrayId insertionId,
			final Resolution resolution,
			final Envelope originalEnvelope,
			final Geometry footprintWorldGeometry,
			final Geometry footprintScreenGeometry,
			final Map properties ) {
		this.gridCoverage = gridCoverage;
		this.insertionId = insertionId;
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

	public ByteArrayId getInsertionId() {
		return insertionId;
	}

	public Resolution getResolution() {
		return resolution;
	}

	public GridCoverage getOriginalCoverage() {
		return gridCoverage;
	}

	public Envelope getOriginalEnvelope() {
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
	public GridGeometry getOverviewGridGeometry(
			final int index )
			throws IndexOutOfBoundsException {
		return gridCoverage.getOverviewGridGeometry(index);
	}

	@Override
	public GridCoverage getOverview(
			final int index )
			throws IndexOutOfBoundsException {
		return gridCoverage.getOverview(index);
	}

	@Override
	public CoordinateReferenceSystem getCoordinateReferenceSystem() {
		return gridCoverage.getCoordinateReferenceSystem();
	}

	@Override
	public Envelope getEnvelope() {
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
	public Set<Record> evaluate(
			final DirectPosition p,
			final Collection<String> list )
			throws PointOutsideCoverageException,
			CannotEvaluateException {
		return gridCoverage.evaluate(
				p,
				list);
	}

	@Override
	public RenderedImage getRenderedImage() {
		return gridCoverage.getRenderedImage();
	}

	@Override
	public Object evaluate(
			final DirectPosition point )
			throws PointOutsideCoverageException,
			CannotEvaluateException {
		return gridCoverage.evaluate(point);
	}

	@Override
	public boolean[] evaluate(
			final DirectPosition point,
			final boolean[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public byte[] evaluate(
			final DirectPosition point,
			final byte[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public int[] evaluate(
			final DirectPosition point,
			final int[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public float[] evaluate(
			final DirectPosition point,
			final float[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public double[] evaluate(
			final DirectPosition point,
			final double[] destination )
			throws PointOutsideCoverageException,
			CannotEvaluateException,
			ArrayIndexOutOfBoundsException {
		return gridCoverage.evaluate(
				point,
				destination);
	}

	@Override
	public int getNumSampleDimensions() {
		return gridCoverage.getNumSampleDimensions();
	}

	@Override
	public SampleDimension getSampleDimension(
			final int index )
			throws IndexOutOfBoundsException {
		return gridCoverage.getSampleDimension(index);
	}

	@Override
	public RenderableImage getRenderableImage(
			final int xAxis,
			final int yAxis )
			throws UnsupportedOperationException,
			IndexOutOfBoundsException {
		return gridCoverage.getRenderableImage(
				xAxis,
				yAxis);
	}

}
