package mil.nga.giat.geowave.analytic.spark.sparksql.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.BasicFeatureTypes;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.analytic.spark.sparksql.SimpleFeatureDataType;

public class SchemaConverter
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SchemaConverter.class);

	public static SimpleFeatureType schemaToFeatureType(
			StructType schema,
			String typeName ) {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(typeName);
		typeBuilder.setNamespaceURI(BasicFeatureTypes.DEFAULT_NAMESPACE);
		try {
			typeBuilder.setCRS(CRS.decode(
					"EPSG:4326",
					true));
		}
		catch (final FactoryException e) {
			LOGGER.error(
					e.getMessage(),
					e);
		}

		final AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();

		for (StructField field : schema.fields()) {
			AttributeDescriptor attrDesc = attrDescFromStructField(
					attrBuilder,
					field);

			typeBuilder.add(attrDesc);
		}

		return typeBuilder.buildFeatureType();
	}

	private static AttributeDescriptor attrDescFromStructField(
			AttributeTypeBuilder attrBuilder,
			StructField field ) {
		if (field.name().equals(
				"geom")) {
			return attrBuilder.binding(
					Geometry.class).nillable(
					false).buildDescriptor(
					"geom");
		}
		if (field.dataType() == DataTypes.StringType) {
			return attrBuilder.binding(
					String.class).buildDescriptor(
					field.name());
		}
		else if (field.dataType() == DataTypes.DoubleType) {
			return attrBuilder.binding(
					Double.class).buildDescriptor(
					field.name());
		}
		else if (field.dataType() == DataTypes.FloatType) {
			return attrBuilder.binding(
					Float.class).buildDescriptor(
					field.name());
		}
		else if (field.dataType() == DataTypes.LongType) {
			return attrBuilder.binding(
					Long.class).buildDescriptor(
					field.name());
		}
		else if (field.dataType() == DataTypes.IntegerType) {
			return attrBuilder.binding(
					Integer.class).buildDescriptor(
					field.name());
		}
		else if (field.dataType() == DataTypes.BooleanType) {
			return attrBuilder.binding(
					Boolean.class).buildDescriptor(
					field.name());
		}
		else if (field.dataType() == DataTypes.TimestampType) {
			return attrBuilder.binding(
					Date.class).buildDescriptor(
					field.name());
		}

		return null;
	}

	public static StructType schemaFromFeatureType(
			SimpleFeatureType featureType ) {
		List<StructField> fields = new ArrayList<>();

		for (AttributeDescriptor attrDesc : featureType.getAttributeDescriptors()) {
			SimpleFeatureDataType sfDataType = attrDescToDataType(attrDesc);

			String fieldName = (sfDataType.isGeom() ? "geom" : attrDesc.getName().getLocalPart());

			StructField field = DataTypes.createStructField(
					fieldName,
					sfDataType.getDataType(),
					true);

			fields.add(field);
		}

		if (fields.isEmpty()) {
			LOGGER.error("Feature type produced empty dataframe schema!");
			return null;
		}

		return DataTypes.createStructType(fields);
	}

	private static SimpleFeatureDataType attrDescToDataType(
			AttributeDescriptor attrDesc ) {
		boolean isGeom = false;
		DataType dataTypeOut = DataTypes.NullType;

		if (attrDesc.getType().getBinding().equals(
				String.class)) {

			dataTypeOut = DataTypes.StringType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Double.class)) {
			dataTypeOut = DataTypes.DoubleType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Float.class)) {
			dataTypeOut = DataTypes.FloatType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Long.class)) {
			dataTypeOut = DataTypes.LongType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Integer.class)) {
			dataTypeOut = DataTypes.IntegerType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Boolean.class)) {
			dataTypeOut = DataTypes.BooleanType;
		}
		else if (attrDesc.getType().getBinding().equals(
				Date.class)) {
			dataTypeOut = DataTypes.TimestampType;
		}

		// Custom geometry types get WKB encoding
		else if (Geometry.class.isAssignableFrom(attrDesc.getType().getBinding())) {
			dataTypeOut = DataTypes.StringType;
			isGeom = true;
		}

		return new SimpleFeatureDataType(
				dataTypeOut,
				isGeom);
	}
}
