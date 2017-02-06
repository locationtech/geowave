package mil.nga.giat.geowave.analytic.mapreduce.clustering;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.analytic.AnalyticFeature;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.extract.EmptyDimensionExtractor;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.GeoWaveReducer;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.geotools.feature.type.BasicFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Remove duplicate input objects and write out as a simple feature with
 * geometry projected onto CRS EPSG:4326. The output feature contains the ID of
 * the originating object. The intent is to create a light weight uniform object
 * that reuses GeoWave data formats to feed analytic processes.
 * 
 * If the input object does not require adjustment after de-duplication, use
 * {@link mil.nga.giat.geowave.accumulo.mapreduce.dedupe.GeoWaveDedupReducer}
 * 
 * OutputFeature Attributes, see
 * {@link mil.nga.giat.geowave.analytic.AnalyticFeature.ClusterFeatureAttribute}
 * 
 * Context configuration parameters include:
 * 
 * @formatter:off
 * 
 * 
 *                "SimpleFeatureOutputReducer.Extract.DimensionExtractClass" ->
 *                {@link DimensionExtractor} to extract non-geometric dimensions
 * 
 *                "SimpleFeatureOutputReducer.Extract.OutputDataTypeId" -> the
 *                name of the output SimpleFeature data type
 * 
 *                "SimpleFeatureOutputReducer.Global.BatchId" ->the id of the
 *                batch; defaults to current time in millis (for range
 *                comparisons)
 * 
 * 
 * @formatter:on
 */

public class SimpleFeatureOutputReducer extends
		GeoWaveReducer
{
	protected DimensionExtractor<Object> dimExtractor;
	protected String outputDataTypeID;
	protected String batchID;
	protected String groupID;
	protected FeatureDataAdapter outputAdapter;

	protected static final Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureOutputReducer.class);

	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final ReduceContext<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException {
		final Iterator<Object> valIt = values.iterator();
		if (valIt.hasNext()) {
			key.setAdapterId(outputAdapter.getAdapterId());
			final SimpleFeature feature = getSimpleFeature(
					key,
					valIt.next());
			context.write(
					key,
					feature);
		}
	}

	private SimpleFeature getSimpleFeature(
			final GeoWaveInputKey key,
			final Object entry ) {
		final Geometry geometry = dimExtractor.getGeometry(entry);
		final double[] extraDims = dimExtractor.getDimensions(entry);

		final String inputID = StringUtils.stringFromBinary(key.getDataId().getBytes());
		final SimpleFeature pointFeature = AnalyticFeature.createGeometryFeature(
				outputAdapter.getFeatureType(),
				batchID,
				inputID,
				inputID,
				groupID,
				0.0,
				geometry,
				dimExtractor.getDimensionNames(),
				extraDims,
				1,
				1,
				0);

		return pointFeature;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		final ScopedJobConfiguration config = new ScopedJobConfiguration(
				context.getConfiguration(),
				SimpleFeatureOutputReducer.class);

		outputDataTypeID = config.getString(
				ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
				"reduced_features");

		batchID = config.getString(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());

		groupID = config.getString(
				ExtractParameters.Extract.GROUP_ID,
				UUID.randomUUID().toString());

		try {
			dimExtractor = config.getInstance(
					ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS,
					DimensionExtractor.class,
					EmptyDimensionExtractor.class);
		}
		catch (final Exception e1) {
			LOGGER.warn(
					"Failed to instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
							SimpleFeatureOutputReducer.class,
							ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS),
					e1);
			throw new IOException(
					"Invalid configuration for " + GeoWaveConfiguratorBase.enumToConfKey(
							SimpleFeatureOutputReducer.class,
							ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS));
		}

		outputAdapter = AnalyticFeature.createGeometryFeatureAdapter(
				outputDataTypeID,
				dimExtractor.getDimensionNames(),
				config.getString(
						ExtractParameters.Extract.DATA_NAMESPACE_URI,
						BasicFeatureTypes.DEFAULT_NAMESPACE),
				ClusteringUtils.CLUSTERING_CRS);

	}
}
