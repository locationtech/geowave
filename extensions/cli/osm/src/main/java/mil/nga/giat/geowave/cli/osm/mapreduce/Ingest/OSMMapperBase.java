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
package mil.nga.giat.geowave.cli.osm.mapreduce.Ingest;

import java.io.IOException;
import java.util.Calendar;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Constants;
import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Schema;
import mil.nga.giat.geowave.cli.osm.types.TypeUtils;
import mil.nga.giat.geowave.cli.osm.types.generated.LongArray;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class OSMMapperBase<T> extends
		Mapper<AvroKey<T>, NullWritable, Text, Mutation>
{

	private static final Logger log = LoggerFactory.getLogger(OSMMapperBase.class);

	protected final FieldWriter<?, Long> longWriter = FieldUtils.getDefaultWriterForClass(Long.class);
	protected final FieldWriter<?, Integer> intWriter = FieldUtils.getDefaultWriterForClass(Integer.class);
	protected final FieldWriter<?, String> stringWriter = FieldUtils.getDefaultWriterForClass(String.class);
	protected final FieldWriter<?, Double> doubleWriter = FieldUtils.getDefaultWriterForClass(Double.class);
	protected final FieldWriter<?, Boolean> booleanWriter = FieldUtils.getDefaultWriterForClass(Boolean.class);
	protected final FieldWriter<?, Calendar> calendarWriter = FieldUtils.getDefaultWriterForClass(Calendar.class);

	protected ColumnVisibility _visibility = new ColumnVisibility(
			"public".getBytes(Constants.CHARSET));

	protected Text _tableName = new Text(
			"OSM");

	protected byte[] getIdHash(
			long id ) {
		return Schema.getIdHash(id);
	}

	protected void put(
			Mutation m,
			byte[] cf,
			byte[] cq,
			Long val ) {
		if (val != null) {
			m.put(
					cf,
					cq,
					_visibility,
					longWriter.writeField(val));
		}
	}

	protected void put(
			Mutation m,
			byte[] cf,
			byte[] cq,
			Integer val ) {
		if (val != null) {
			m.put(
					cf,
					cq,
					_visibility,
					intWriter.writeField(val));
		}
	}

	protected void put(
			Mutation m,
			byte[] cf,
			byte[] cq,
			Double val ) {
		if (val != null) {
			m.put(
					cf,
					cq,
					_visibility,
					doubleWriter.writeField(val));
		}
	}

	protected void put(
			Mutation m,
			byte[] cf,
			byte[] cq,
			String val ) {
		if (val != null) {
			m.put(
					cf,
					cq,
					_visibility,
					stringWriter.writeField(val));
		}
	}

	protected void put(
			Mutation m,
			byte[] cf,
			byte[] cq,
			CharSequence val ) {
		if (val != null) {
			m.put(
					cf,
					cq,
					_visibility,
					stringWriter.writeField(val.toString()));
		}
	}

	protected void put(
			Mutation m,
			byte[] cf,
			byte[] cq,
			Boolean val ) {
		if (val != null) {
			m.put(
					cf,
					cq,
					_visibility,
					booleanWriter.writeField(val));
		}
	}

	protected void put(
			Mutation m,
			byte[] cf,
			byte[] cq,
			Calendar val ) {
		if (val != null) {
			m.put(
					cf,
					cq,
					_visibility,
					calendarWriter.writeField(val));
		}
	}

	protected void put(
			Mutation m,
			byte[] cf,
			byte[] cq,
			LongArray val ) {
		if (val != null) {
			try {
				m.put(
						cf,
						cq,
						_visibility,
						TypeUtils.serializeLongArray(val));
			}
			catch (IOException e) {
				log.error(
						"Unable to serialize LongArray instance",
						e);
			}
		}
	}

	@Override
	public void setup(
			Context context )
			throws IOException,
			InterruptedException {
		String tn = context.getConfiguration().get(
				"tableName");
		if (tn != null && !tn.isEmpty()) {
			_tableName.set(tn);
		}
		String visibility = context.getConfiguration().get(
				"osmVisibility");
		if (visibility == null) {
			visibility = "";
		}

		_visibility = new ColumnVisibility(
				visibility.getBytes(Constants.CHARSET));
	}

}
