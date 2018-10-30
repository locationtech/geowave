/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.cli.osm.mapreduce.Ingest;

import java.io.IOException;
import java.util.Calendar;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.Constants;
import org.locationtech.geowave.cli.osm.accumulo.osmschema.Schema;
import org.locationtech.geowave.cli.osm.types.TypeUtils;
import org.locationtech.geowave.cli.osm.types.generated.LongArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			final long id ) {
		return Schema.getIdHash(id);
	}

	protected void put(
			final Mutation m,
			final String cf,
			final String cq,
			final Long val ) {
		if (val != null) {
			m.put(
					StringUtils.stringToBinary(cf),
					StringUtils.stringToBinary(cq),
					_visibility,
					longWriter.writeField(val));
		}
	}

	protected void put(
			final Mutation m,
			final String cf,
			final String cq,
			final Integer val ) {
		if (val != null) {
			m.put(
					StringUtils.stringToBinary(cf),
					StringUtils.stringToBinary(cq),
					_visibility,
					intWriter.writeField(val));
		}
	}

	protected void put(
			final Mutation m,
			final String cf,
			final String cq,
			final Double val ) {
		if (val != null) {
			m.put(
					StringUtils.stringToBinary(cf),
					StringUtils.stringToBinary(cq),
					_visibility,
					doubleWriter.writeField(val));
		}
	}

	protected void put(
			final Mutation m,
			final String cf,
			final String cq,
			final String val ) {
		if (val != null) {
			m.put(
					StringUtils.stringToBinary(cf),
					StringUtils.stringToBinary(cq),
					_visibility,
					stringWriter.writeField(val));
		}
	}

	protected void put(
			final Mutation m,
			final String cf,
			final String cq,
			final CharSequence val ) {
		if (val != null) {
			m.put(
					StringUtils.stringToBinary(cf),
					StringUtils.stringToBinary(cq),
					_visibility,
					stringWriter.writeField(val.toString()));
		}
	}

	protected void put(
			final Mutation m,
			final String cf,
			final String cq,
			final Boolean val ) {
		if (val != null) {
			m.put(
					StringUtils.stringToBinary(cf),
					StringUtils.stringToBinary(cq),
					_visibility,
					booleanWriter.writeField(val));
		}
	}

	protected void put(
			final Mutation m,
			final String cf,
			final String cq,
			final Calendar val ) {
		if (val != null) {
			m.put(
					StringUtils.stringToBinary(cf),
					StringUtils.stringToBinary(cq),
					_visibility,
					calendarWriter.writeField(val));
		}
	}

	protected void put(
			final Mutation m,
			final String cf,
			final String cq,
			final LongArray val ) {
		if (val != null) {
			try {
				m.put(
						StringUtils.stringToBinary(cf),
						StringUtils.stringToBinary(cq),
						_visibility,
						TypeUtils.serializeLongArray(val));
			}
			catch (final IOException e) {
				log.error(
						"Unable to serialize LongArray instance",
						e);
			}
		}
	}

	@Override
	public void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		final String tn = context.getConfiguration().get(
				"tableName");
		if ((tn != null) && !tn.isEmpty()) {
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
