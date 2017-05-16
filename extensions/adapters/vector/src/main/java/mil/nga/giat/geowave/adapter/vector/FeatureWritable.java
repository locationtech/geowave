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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Writable;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * This class is used by FeatureDataAdapter to persist SimpleFeature and its
 * SimpleFeatureType. The attribute types of the feature must be understood
 * before the feature can be deserialized so therefore each SimpleFeature
 * serializes its type.
 * 
 * NOTE: This class caches feature type information. If the feature type
 * changes, then the cache should be emptied using the clearCache() method.
 */
public class FeatureWritable implements
		Writable,
		java.io.Serializable
{
	private static final Map<Pair<String, String>, SimpleFeatureType> FeatureTypeCache = new ConcurrentHashMap<Pair<String, String>, SimpleFeatureType>();
	/**
	 * 
	 */
	private static final long serialVersionUID = 286616522680871139L;
	private SimpleFeatureType featureType;
	private SimpleFeature feature;

	public FeatureWritable() {}

	public FeatureWritable(
			final SimpleFeatureType featureType ) {
		this.featureType = featureType;
	}

	public FeatureWritable(
			final SimpleFeatureType featureType,
			final SimpleFeature feature ) {
		this.featureType = featureType;
		this.feature = feature;
	}

	public SimpleFeature getFeature() {
		return feature;
	}

	public void setFeature(
			SimpleFeature feature ) {
		this.feature = feature;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		try {
			String ns = input.readUTF();
			featureType = FeatureDataUtils.decodeType(
					"-".equals(ns) ? "" : ns,
					input.readUTF(),
					input.readUTF(),
					input.readUTF());
		}
		catch (final SchemaException e) {
			throw new IOException(
					"Failed to parse the encoded feature type",
					e);
		}
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				featureType);
		// read the fid
		final String fid = input.readUTF();
		// read the other attributes, build the feature
		for (final AttributeDescriptor ad : featureType.getAttributeDescriptors()) {
			final Object att = readAttribute(
					ad,
					input);
			builder.add(att);
		}

		// build the feature
		feature = builder.buildFeature(fid);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		output
				.writeUTF(featureType.getName().getNamespaceURI() == null ? "-" : featureType
						.getName()
						.getNamespaceURI());
		output.writeUTF(featureType.getTypeName());
		output.writeUTF(DataUtilities.encodeType(featureType));
		output.writeUTF(FeatureDataUtils.getAxis(featureType.getCoordinateReferenceSystem()));

		// write feature id
		output.writeUTF(feature.getID());
		// write the attributes
		for (final AttributeDescriptor ad : featureType.getAttributeDescriptors()) {
			final Object value = feature.getAttribute(ad.getLocalName());
			writeAttribute(
					output,
					ad,
					value);
		}
	}

	static void writeAttribute(
			final DataOutput output,
			final AttributeDescriptor ad,
			final Object value )
			throws IOException {
		if (value == null) {
			// null marker
			output.writeBoolean(true);
		}
		else {
			// not null, write the contents. This one requires some explanation.
			// We are not writing any type metadata in the stream for the types
			// we can optimize (primitives, numbers, strings and the like). This
			// means we have to be 100% sure the class we're writing is actually
			// the one we can optimize for, and not some subclass. Thus, we are
			// authorized to use identity comparison instead of isAssignableFrom
			// or equality, when we read back it must be as if we did not
			// serialize stuff at all
			output.writeBoolean(false);
			final Class<?> binding = ad.getType().getBinding();
			if (binding == Boolean.class) {
				output.writeBoolean((Boolean) value);
			}
			else if ((binding == Byte.class) || (binding == byte.class)) {
				output.writeByte((Byte) value);
			}
			else if ((binding == Short.class) || (binding == short.class)) {
				output.writeShort((Short) value);
			}
			else if ((binding == Integer.class) || (binding == int.class)) {
				output.writeInt((Integer) value);
			}
			else if ((binding == Long.class) || (binding == long.class)) {
				output.writeLong((Long) value);
			}
			else if ((binding == Float.class) || (binding == float.class)) {
				output.writeFloat((Float) value);
			}
			else if ((binding == Double.class) || (binding == double.class)) {
				output.writeDouble((Double) value);
			}
			else if (binding == String.class) {
				output.writeUTF((String) value);
			}
			else if ((binding == java.sql.Date.class) || (binding == java.sql.Time.class)
					|| (binding == java.sql.Timestamp.class) || (binding == java.util.Date.class)) {
				output.writeLong(((Date) value).getTime());
			}
			else if (Geometry.class.isAssignableFrom(binding)) {
				final WKBWriter writer = new WKBWriter();
				final byte[] buffer = writer.write((Geometry) value);
				final int length = buffer.length;
				output.writeInt(length);
				output.write(buffer);
			}
			else {
				// can't optimize, in this case we use an ObjectOutputStream to
				// write out full metadata
				final ByteArrayOutputStream bos = new ByteArrayOutputStream();
				final ObjectOutputStream oos = new ObjectOutputStream(
						bos);
				oos.writeObject(value);
				oos.flush();
				final byte[] bytes = bos.toByteArray();
				output.writeInt(bytes.length);
				output.write(bytes);
			}
		}
	}

	/**
	 * Reads the attributes.
	 * 
	 * @param ad
	 * @return
	 * @throws IOException
	 */
	Object readAttribute(
			final AttributeDescriptor ad,
			final DataInput input )
			throws IOException {
		final boolean isNull = input.readBoolean();
		if (isNull) {
			return null;
		}
		else {
			final Class<?> binding = ad.getType().getBinding();
			if (binding == Boolean.class) {
				return input.readBoolean();
			}
			else if ((binding == Byte.class) || (binding == byte.class)) {
				return input.readByte();
			}
			else if ((binding == Short.class) || (binding == short.class)) {
				return input.readShort();
			}
			else if ((binding == Integer.class) || (binding == int.class)) {
				return input.readInt();
			}
			else if ((binding == Long.class) || (binding == long.class)) {
				return input.readLong();
			}
			else if ((binding == Float.class) || (binding == float.class)) {
				return input.readFloat();
			}
			else if ((binding == Double.class) || (binding == double.class)) {
				return input.readDouble();
			}
			else if (binding == String.class) {
				return input.readUTF();
			}
			else if (binding == java.sql.Date.class) {
				return new java.sql.Date(
						input.readLong());
			}
			else if (binding == java.sql.Time.class) {
				return new java.sql.Time(
						input.readLong());
			}
			else if (binding == java.sql.Timestamp.class) {
				return new java.sql.Timestamp(
						input.readLong());
			}
			else if (binding == java.util.Date.class) {
				return new java.util.Date(
						input.readLong());
			}
			else if (Geometry.class.isAssignableFrom(binding)) {
				final WKBReader reader = new WKBReader();
				final int length = input.readInt();
				final byte[] buffer = new byte[length];
				input.readFully(buffer);
				try {
					return reader.read(buffer);
				}
				catch (final ParseException e) {
					throw new IOException(
							"Failed to parse the geometry WKB",
							e);
				}
			}
			else {
				final int length = input.readInt();
				final byte[] buffer = new byte[length];
				input.readFully(buffer);
				final ByteArrayInputStream bis = new ByteArrayInputStream(
						buffer);
				final ObjectInputStream ois = new ObjectInputStream(
						bis);
				try {
					return ois.readObject();
				}
				catch (final ClassNotFoundException e) {
					throw new IOException(
							"Could not read back object",
							e);
				}
			}
		}
	}

	private void writeObject(
			java.io.ObjectOutputStream out )
			throws IOException {
		this.write(out);
	}

	private void readObject(
			java.io.ObjectInputStream in )
			throws IOException,
			ClassNotFoundException {
		this.readFields(in);
	}

	public static final void clearCache() {
		FeatureTypeCache.clear();
	}

	public static final void cache(
			SimpleFeatureType featureType ) {
		final Pair<String, String> id = Pair.of(
				featureType.getName().getNamespaceURI() == null ? "" : featureType.getName().getNamespaceURI(),
				featureType.getTypeName());
		FeatureTypeCache.put(
				id,
				featureType);
	}
}
