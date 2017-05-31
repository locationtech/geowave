package mil.nga.giat.geowave.analytic.javaspark.sparksql;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.analytic.javaspark.sparksql.udf.GeomFunctionRegistry;
import mil.nga.giat.geowave.analytic.javaspark.sparksql.util.SchemaConverter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class SimpleFeatureDataFrame
{
	private static Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureDataFrame.class);

	private final SparkSession sparkSession;
	private final SimpleFeatureType featureType;
	private StructType schema;
	private JavaRDD<Row> rowRDD = null;
	private Dataset<Row> dataFrame = null;

	public SimpleFeatureDataFrame(
			final SparkSession sparkSession,
			final DataStorePluginOptions dataStore,
			final ByteArrayId adapterId ) {
		this.sparkSession = sparkSession;

		featureType = FeatureDataUtils.getFeatureType(
				dataStore,
				adapterId);

		schema = SchemaConverter.schemaFromFeatureType(featureType);

		GeomFunctionRegistry.registerGeometryFunctions(sparkSession);
	}

	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	public StructType getSchema() {
		return schema;
	}

	public JavaRDD<Row> getRowRDD() {
		return rowRDD;
	}

	public Dataset<Row> getDataFrame(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		if (rowRDD == null) {
			SimpleFeatureMapper mapper = new SimpleFeatureMapper(
					schema);

			rowRDD = pairRDD.values().map(
					mapper);
		}

		if (dataFrame == null) {
			dataFrame = sparkSession.createDataFrame(
					rowRDD,
					schema);
		}

		return dataFrame;
	}

	public Dataset<Row> resetDataFrame(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> pairRDD ) {
		rowRDD = null;
		dataFrame = null;

		return getDataFrame(pairRDD);
	}
}
