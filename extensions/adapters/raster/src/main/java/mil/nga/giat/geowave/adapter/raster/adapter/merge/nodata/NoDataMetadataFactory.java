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
package mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMetadata.SampleIndex;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class NoDataMetadataFactory
{
	private static class NoDataSummary
	{
		private final Set<SampleIndex> indices;
		private final double[][] usedNoDataValues;

		public NoDataSummary(
				final Set<SampleIndex> indices,
				final double[][] usedNoDataValues ) {
			this.indices = indices;
			this.usedNoDataValues = usedNoDataValues;
		}
	}

	private static final int MAX_LIST_NO_DATA = 20;

	public static NoDataMetadata createMetadata(
			final double[][] allNoDataValues,
			final Geometry shape,
			final Raster data ) {
		final NoDataSummary noDataSummary = getNoDataSummary(
				allNoDataValues,
				shape,
				data);
		return createMetadata(
				noDataSummary,
				new Geometry[] {
					shape
				},
				data.getWidth(),
				data.getHeight());
	}

	public static NoDataMetadata mergeMetadata(
			final NoDataMetadata noDataMetadata1,
			final WritableRaster raster1,
			final NoDataMetadata noDataMetadata2,
			final WritableRaster raster2 ) {
		if ((noDataMetadata1 == null) || (noDataMetadata2 == null)) {
			// this implies that there is no nodata values in one of the rasters
			// so there is no nodata values in the merge
			return null;
		}
		final Set<SampleIndex> noDataIndices1 = noDataMetadata1.getNoDataIndices();
		final Set<SampleIndex> noDataIndices2 = noDataMetadata2.getNoDataIndices();
		if ((noDataIndices1 != null) && (noDataIndices2 != null)) {
			// simple case, just take the intersection of the sets
			noDataIndices2.retainAll(noDataIndices1);
			return new NoDataBySampleIndex(
					noDataIndices2);
		}
		else if (noDataIndices1 != null) {
			// just determine which of the no data indices are covered by the
			// second set of metadata and remove them
			return mergeMetadataBySummary(
					noDataIndices1,
					noDataMetadata2,
					raster2);
		}
		else if (noDataIndices2 != null) {
			// just determine which of the no data indices are covered by the
			// first set of metadata and remove them
			return mergeMetadataBySummary(
					noDataIndices2,
					noDataMetadata1,
					raster1);
		}
		else if ((noDataMetadata1 instanceof NoDataByFilter) && (noDataMetadata2 instanceof NoDataByFilter)) {
			final NoDataByFilter noDataByFilter1 = ((NoDataByFilter) noDataMetadata1);
			final NoDataByFilter noDataByFilter2 = ((NoDataByFilter) noDataMetadata2);

			final double[][] noDataPerBand1 = noDataByFilter1.getNoDataPerBand();
			final double[][] noDataPerBand2 = noDataByFilter2.getNoDataPerBand();
			// union the no data values from each filter
			final int numBands = Math.min(
					noDataPerBand1.length,
					noDataPerBand2.length);
			final double[][] allNoDataValues = new double[numBands][];
			for (int b = 0; b < numBands; b++) {
				final Set<Double> noDataValuesInBand = new HashSet<Double>();
				if (noDataPerBand1[b] != null) {
					for (final double noDataValue : noDataPerBand1[b]) {
						noDataValuesInBand.add(noDataValue);
					}
				}
				if (noDataPerBand2[b] != null) {
					for (final double noDataValue : noDataPerBand2[b]) {
						noDataValuesInBand.add(noDataValue);
					}
				}
				allNoDataValues[b] = new double[noDataValuesInBand.size()];
				int i = 0;
				final Iterator<Double> it = noDataValuesInBand.iterator();
				while (it.hasNext()) {
					allNoDataValues[b][i++] = it.next();
				}
			}
			return mergeMetadataBySummary(
					allNoDataValues,
					noDataByFilter1,
					raster1,
					noDataByFilter2,
					raster2);
		}
		else {
			// this should never happen because the only implementations of
			// metadata are by index or by filter but just in case iteratively
			// go through every sample, determine if its covered by the first or
			// the second set of metadata and use the indices
			return exhaustiveMergeMetadata(
					noDataMetadata1,
					raster1,
					noDataMetadata2,
					raster2);
		}
	}

	private static NoDataMetadata createMetadata(
			final NoDataSummary noDataSummary,
			final Geometry[] shapes,
			final int width,
			final int height ) {
		if (noDataSummary.indices.size() > MAX_LIST_NO_DATA) {
			Geometry finalShape;
			if ((shapes == null) || (shapes.length == 0)) {
				finalShape = null;
			}
			else {
				finalShape = shapes[0];
				if ((shapes.length > 1) && (finalShape != null)) {
					for (int i = 1; i < shapes.length; i++) {
						if (shapes[i] == null) {
							finalShape = null;
							break;
						}
						else {
							finalShape = finalShape.union(shapes[i]);
						}
					}
				}
			}
			if ((finalShape != null) && finalShape.covers(new GeometryFactory().toGeometry(new Envelope(
					0,
					width,
					0,
					height)))) {
				// if the coverage of this geometric union ever gets to the
				// point that it fully covers the raster, stop storing it and
				// just set the geometry to null
				finalShape = null;
			}
			return new NoDataByFilter(
					finalShape,
					noDataSummary.usedNoDataValues);
		}
		else if (!noDataSummary.indices.isEmpty()) {
			// just go through every raster sample and determine whether it
			// qualifies as null data
			return new NoDataBySampleIndex(
					noDataSummary.indices);
		}
		else {
			// the "no data" samples in the dataset must be 0, so just return
			// null for the metadata
			return null;
		}
	}

	private static NoDataMetadata mergeMetadataBySummary(
			final Set<SampleIndex> noDataIndices,
			final NoDataMetadata noDataMetadata,
			final WritableRaster raster ) {
		final Iterator<SampleIndex> indices = noDataIndices.iterator();
		while (indices.hasNext()) {
			final SampleIndex index = indices.next();
			if (!noDataMetadata.isNoData(
					index,
					raster.getSampleDouble(
							index.getX(),
							index.getY(),
							index.getBand()))) {
				indices.remove();
			}
		}
		return new NoDataBySampleIndex(
				noDataIndices);
	}

	private static NoDataMetadata exhaustiveMergeMetadata(
			final NoDataMetadata noDataMetadata1,
			final WritableRaster raster1,
			final NoDataMetadata noDataMetadata2,
			final WritableRaster raster2 ) {
		final int width = Math.min(
				raster1.getWidth(),
				raster2.getWidth());
		final int height = Math.min(
				raster1.getHeight(),
				raster2.getHeight());
		final int numBands = Math.min(
				raster1.getNumBands(),
				raster2.getNumBands());
		final Set<SampleIndex> indices = new HashSet<SampleIndex>();
		for (int b = 0; b < numBands; b++) {
			for (int x = 0; x < width; x++) {
				for (int y = 0; y < height; y++) {
					final SampleIndex index = new SampleIndex(
							x,
							y,
							b);
					if (noDataMetadata1.isNoData(
							index,
							raster1.getSampleDouble(
									x,
									y,
									b)) && noDataMetadata2.isNoData(
							index,
							raster2.getSampleDouble(
									x,
									y,
									b))) {
						indices.add(index);
					}
				}
			}
		}
		return new NoDataBySampleIndex(
				indices);
	}

	private static NoDataMetadata mergeMetadataBySummary(
			final double[][] allNoDataValues,
			final NoDataByFilter noDataMetadata1,
			final WritableRaster raster1,
			final NoDataByFilter noDataMetadata2,
			final WritableRaster raster2 ) {
		final NoDataSummary noDataSummary = getNoDataSummary(
				allNoDataValues,
				noDataMetadata1,
				raster1,
				noDataMetadata2,
				raster2);
		return createMetadata(
				noDataSummary,
				new Geometry[] {
					noDataMetadata1.getShape(),
					noDataMetadata2.getShape()
				},
				raster2.getWidth(), // both rasters better be the same
									// dimensions
				raster2.getHeight());
	}

	private static NoDataSummary getNoDataSummary(
			final double[][] allNoDataValues,
			final NoDataByFilter noDataMetadata1,
			final WritableRaster raster1,
			final NoDataByFilter noDataMetadata2,
			final WritableRaster raster2 ) {
		final int width = Math.min(
				raster1.getWidth(),
				raster2.getWidth());
		final int height = Math.min(
				raster1.getHeight(),
				raster2.getHeight());
		final int numBands = Math.min(
				raster1.getNumBands(),
				raster2.getNumBands());
		return getNoDataSummary(
				allNoDataValues,
				new MultiShape(
						new Geometry[] {
							noDataMetadata1.getShape(),
							noDataMetadata2.getShape()
						}),
				new MultiRaster(
						new Raster[] {
							raster1,
							raster2
						}),
				width,
				height,
				numBands);
	}

	private static NoDataSummary getNoDataSummary(
			final double[][] allNoDataValues,
			final Geometry shape,
			final Raster data ) {
		return getNoDataSummary(
				allNoDataValues,
				new SingleShape(
						shape),
				new SingleRaster(
						data),
				data.getWidth(),
				data.getHeight(),
				data.getNumBands());
	}

	private static NoDataSummary getNoDataSummary(
			final double[][] allNoDataValues,
			final NoDataByCoordinate shape,
			final NoDataBySample data,
			final int width,
			final int height,
			final int numBands ) {

		final Set<Double>[] noDataValuesPerBand;
		boolean skipNoData;

		final Set<SampleIndex> indices = new HashSet<SampleIndex>();
		if (allNoDataValues == null) {
			skipNoData = true;
			noDataValuesPerBand = null;
			if (shape == null) {
				return new NoDataSummary(
						indices,
						new double[][] {});
			}
		}
		else {
			noDataValuesPerBand = new Set[numBands];
			for (int b = 0; b < numBands; b++) {
				noDataValuesPerBand[b] = new HashSet<Double>();
			}
			skipNoData = false;
		}

		for (int x = 0; x < width; x++) {
			for (int y = 0; y < height; y++) {
				if (shape.isNoData(
						x,
						y)) {
					for (int b = 0; b < numBands; b++) {
						indices.add(new SampleIndex(
								x,
								y,
								b));
					}
					// this will ignore the no data values for this x,y
					// which should be fine because the shape will
					// always classify this x,y as "no data"
				}
				else if (!skipNoData) {
					for (int b = 0; b < numBands; b++) {
						if (allNoDataValues[b] == null) {
							continue;
						}
						else {
							final double[] samples = data.getSampleValues(
									x,
									y,
									b);
							for (int i = 0; i < allNoDataValues[b].length; i++) {
								// if a single sample is not a "no data" value
								// then it is valid
								boolean noData = true;
								for (final double sample : samples) {
									// we wrap it with Object equality to make
									// sure we generically catch special
									// cases, such as NaN and positive and
									// negative infinite
									if (!new Double(
											sample).equals(allNoDataValues[b][i])) {
										noData = false;
										break;
									}
								}
								if (noData) {
									indices.add(new SampleIndex(
											x,
											y,
											b));
									if (noDataValuesPerBand != null && noDataValuesPerBand[b] != null) {
										noDataValuesPerBand[b].add(allNoDataValues[b][i]);
									}
								}
							}
						}
					}
				}
			}
		}

		final double[][] usedNoDataValues;
		if (!skipNoData && noDataValuesPerBand != null) {
			usedNoDataValues = new double[noDataValuesPerBand.length][];
			for (int b = 0; b < noDataValuesPerBand.length; b++) {
				usedNoDataValues[b] = new double[noDataValuesPerBand[b].size()];
				int i = 0;
				final Iterator<Double> noDataValues = noDataValuesPerBand[b].iterator();
				while (noDataValues.hasNext()) {
					usedNoDataValues[b][i++] = noDataValues.next();
				}
			}
		}
		else {
			usedNoDataValues = new double[][] {};
		}
		return new NoDataSummary(
				indices,
				usedNoDataValues);
	}

	private static interface NoDataByCoordinate
	{
		public boolean isNoData(
				int x,
				int y );
	}

	private static interface NoDataBySample
	{
		public double[] getSampleValues(
				int x,
				int y,
				int b );
	}

	private static class SingleShape implements
			NoDataByCoordinate
	{
		private final Geometry shape;

		public SingleShape(
				final Geometry shape ) {
			this.shape = shape;
		}

		@Override
		public boolean isNoData(
				final int x,
				final int y ) {
			return ((shape != null) && !shape.intersects(new GeometryFactory().createPoint(new Coordinate(
					x,
					y))));
		}
	}

	private static class MultiShape implements
			NoDataByCoordinate
	{
		private final Geometry[] shapes;
		private boolean acceptNone = false;

		public MultiShape(
				final Geometry[] shapes ) {
			this.shapes = shapes;
			if ((shapes == null) || (shapes.length == 0)) {
				acceptNone = true;
			}
			else {
				for (final Geometry shape : shapes) {
					if (shape == null) {
						acceptNone = true;
					}
				}
			}
		}

		@Override
		public boolean isNoData(
				final int x,
				final int y ) {
			if (!acceptNone) {
				for (final Geometry shape : shapes) {
					// if any one intersects the point than it is not "no data"
					// based on shape
					if (shape.intersects(new GeometryFactory().createPoint(new Coordinate(
							x,
							y)))) {
						return false;
					}
				}
				return true;
			}
			return false;
		}
	}

	private static class SingleRaster implements
			NoDataBySample
	{
		private final Raster raster;

		public SingleRaster(
				final Raster raster ) {
			this.raster = raster;
		}

		@Override
		public double[] getSampleValues(
				final int x,
				final int y,
				final int b ) {
			return new double[] {
				raster.getSampleDouble(
						x,
						y,
						b)
			};
		}
	}

	private static class MultiRaster implements
			NoDataBySample
	{
		private final Raster[] rasters;

		public MultiRaster(
				final Raster[] rasters ) {
			this.rasters = rasters;
		}

		@Override
		public double[] getSampleValues(
				final int x,
				final int y,
				final int b ) {
			final double[] samples = new double[rasters.length];
			for (int i = 0; i < rasters.length; i++) {
				samples[i] = rasters[i].getSampleDouble(
						x,
						y,
						b);
			}
			return samples;
		}
	}
}
