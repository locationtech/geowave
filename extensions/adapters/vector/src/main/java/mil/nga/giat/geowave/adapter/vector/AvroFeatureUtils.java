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
package mil.nga.giat.geowave.adapter.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import mil.nga.giat.geowave.adapter.vector.avro.AttributeValues;
import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeature;
import mil.nga.giat.geowave.adapter.vector.avro.FeatureDefinition;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class AvroFeatureUtils
{
	private static final WKBWriter WKB_WRITER = new WKBWriter(
			3);

	private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();
	private static final SpecificDatumReader<AvroSimpleFeature> DATUM_READER = new SpecificDatumReader<>();
	private static final WKBReader WKB_READER = new WKBReader();

	private AvroFeatureUtils() {}

	/**
	 * Add the attributes, types and classifications for the SimpleFeatureType
	 * to the provided FeatureDefinition
	 *
	 * @param fd
	 *            - existing Feature Definition (or new one if null)
	 * @param sft
	 *            - SimpleFeatureType of the simpleFeature being serialized
	 * @param defaultClassifications
	 *            - map of attribute names to classification
	 * @param defaultClassification
	 *            - default classification if one could not be found in the map
	 * @return
	 * @throws IOException
	 */
	public static FeatureDefinition buildFeatureDefinition(
			FeatureDefinition fd,
			final SimpleFeatureType sft,
			final Map<String, String> defaultClassifications,
			final String defaultClassification )
			throws IOException {
		if (fd == null) {
			fd = new FeatureDefinition();
		}
		fd.setFeatureTypeName(sft.getTypeName());

		final List<String> attributes = new ArrayList<>(
				sft.getAttributeCount());
		final List<String> types = new ArrayList<>(
				sft.getAttributeCount());
		final List<String> classifications = new ArrayList<>(
				sft.getAttributeCount());

		for (final AttributeDescriptor attr : sft.getAttributeDescriptors()) {
			final String localName = attr.getLocalName();

			attributes.add(localName);
			types.add(attr.getType().getBinding().getCanonicalName());
			classifications.add(getClassification(
					localName,
					defaultClassifications,
					defaultClassification));
		}

		fd.setAttributeNames(attributes);
		fd.setAttributeTypes(types);
		fd.setAttributeDefaultClassifications(classifications);

		return fd;
	}

	/**
	 * If a classification exists for this attribute name then use it If not
	 * then use the provided default classification
	 *
	 * @param localName
	 *            - attribute name
	 * @param defaultClassifications
	 *            - map of attribute names to classification
	 * @param defaultClassification
	 *            - default classification to use if one is not mapped for the
	 *            name provided
	 * @return
	 * @throws IOException
	 */
	private static String getClassification(
			final String localName,
			final Map<String, String> defaultClassifications,
			final String defaultClassification )
			throws IOException {
		String classification;

		if ((defaultClassifications != null) && defaultClassifications.containsKey(localName)) {
			classification = defaultClassifications.get(localName);
		}
		else {
			classification = defaultClassification;
		}

		if (classification == null) {
			throw new IOException(
					"No default classification was provided, and no classification for: '" + localName
							+ "' was provided");
		}

		return classification;
	}

	/**
	 * Create an AttributeValue from the SimpleFeature's attributes
	 *
	 * @param sf
	 * @param sft
	 * @return
	 */
	synchronized public static AttributeValues buildAttributeValue(
			final SimpleFeature sf,
			final SimpleFeatureType sft ) {
		final AttributeValues attributeValue = new AttributeValues();

		final List<ByteBuffer> values = new ArrayList<>(
				sft.getAttributeCount());

		attributeValue.setFid(sf.getID());

		for (final AttributeDescriptor attr : sft.getAttributeDescriptors()) {
			final Object o = sf.getAttribute(attr.getLocalName());
			byte[] bytes;
			if (o instanceof Geometry) {
				bytes = WKB_WRITER.write((Geometry) o);
			}
			else {
				final FieldWriter fw = FieldUtils.getDefaultWriterForClass(attr.getType().getBinding());
				bytes = fw.writeField(o);
			}
			values.add(ByteBuffer.wrap(bytes));
		}
		attributeValue.setValues(values);

		return attributeValue;
	}

	/***
	 * Deserialize byte array into an AvroSimpleFeature then convert to a
	 * SimpleFeature
	 *
	 * @param avroData
	 *            serialized bytes of a AvroSimpleFeature
	 * @return Collection of GeoTools SimpleFeature instances.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws ParseException
	 */
	synchronized public static SimpleFeature deserializeAvroSimpleFeature(
			final byte[] avroData )
			throws IOException,
			ClassNotFoundException,
			ParseException {
		// Deserialize
		final AvroSimpleFeature sfc = deserializeASF(
				avroData,
				null);
		final FeatureDefinition featureDefinition = sfc.getFeatureType();
		return avroSimpleFeatureToGTSimpleFeature(
				avroFeatureDefinitionToGTSimpleFeatureType(featureDefinition),
				featureDefinition.getAttributeTypes(),
				sfc.getValue());
	}

	public static SimpleFeatureType avroFeatureDefinitionToGTSimpleFeatureType(
			final FeatureDefinition featureDefinition )
			throws ClassNotFoundException {
		final SimpleFeatureTypeBuilder sftb = new SimpleFeatureTypeBuilder();
		sftb.setCRS(GeometryUtils.DEFAULT_CRS);
		sftb.setName(featureDefinition.getFeatureTypeName());
		final List<String> featureTypes = featureDefinition.getAttributeTypes();
		final List<String> featureNames = featureDefinition.getAttributeNames();
		for (int i = 0; i < featureDefinition.getAttributeNames().size(); i++) {
			final String type = featureTypes.get(i);
			final String name = featureNames.get(i);
			final Class<?> c = Class.forName(type);
			sftb.add(
					name,
					c);
		}
		return sftb.buildFeatureType();
	}

	public static SimpleFeature avroSimpleFeatureToGTSimpleFeature(
			final SimpleFeatureType type,
			final List<String> attributeTypes,
			final AttributeValues attributeValues )
			throws IOException,
			ClassNotFoundException,
			ParseException {
		// Convert
		SimpleFeature simpleFeature;

		final SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(
				type);

		// null values should still take a place in the array - check
		Preconditions.checkArgument(attributeTypes.size() == attributeValues.getValues().size());
		for (int i = 0; i < attributeValues.getValues().size(); i++) {
			final ByteBuffer val = attributeValues.getValues().get(
					i);

			if (attributeTypes.get(
					i).equals(
					"com.vividsolutions.jts.geom.Geometry")) {
				sfb.add(WKB_READER.read(val.array()));
			}
			else {
				final FieldReader<?> fr = FieldUtils.getDefaultReaderForClass(Class.forName(attributeTypes.get(i)));
				sfb.add(fr.readField(val.array()));
			}
		}

		simpleFeature = sfb.buildFeature(attributeValues.getFid());
		return simpleFeature;
	}

	/***
	 * Deserialize byte stream into an AvroSimpleFeature
	 *
	 * @param avroData
	 *            serialized bytes of AvroSimpleFeature
	 * @param avroObjectToReuse
	 *            null or AvroSimpleFeature instance to be re-used. If null a
	 *            new object will be allocated.
	 * @return instance of AvroSimpleFeature with values parsed from avroData
	 * @throws IOException
	 */
	private static AvroSimpleFeature deserializeASF(
			final byte[] avroData,
			AvroSimpleFeature avroObjectToReuse )
			throws IOException {
		final BinaryDecoder decoder = DECODER_FACTORY.binaryDecoder(
				avroData,
				null);
		if (avroObjectToReuse == null) {
			avroObjectToReuse = new AvroSimpleFeature();
		}

		DATUM_READER.setSchema(avroObjectToReuse.getSchema());
		return DATUM_READER.read(
				avroObjectToReuse,
				decoder);
	}
}
