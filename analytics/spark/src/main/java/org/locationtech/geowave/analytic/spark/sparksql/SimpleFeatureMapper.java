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
package org.locationtech.geowave.analytic.spark.sparksql;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings
public class SimpleFeatureMapper implements
		Function<SimpleFeature, Row>
{
	private static Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureDataFrame.class);

	private final StructType schema;

	public SimpleFeatureMapper(
			StructType schema ) {
		this.schema = schema;
	}

	@Override
	public Row call(
			SimpleFeature feature )
			throws Exception {
		Object[] fields = new Serializable[schema.size()];

		for (int i = 0; i < schema.size(); i++) {
			Object fieldObj = feature.getAttribute(i);
			if (fieldObj != null) {
				StructField structField = schema.apply(i);
				if (structField.name().equals(
						"geom")) {
					fields[i] = fieldObj;
				}
				else if (structField.dataType() == DataTypes.TimestampType) {
					fields[i] = new Timestamp(
							((Date) fieldObj).getTime());
				}
				else if (structField.dataType() != null) {
					fields[i] = fieldObj;
				}
				else {
					LOGGER.error("Unexpected attribute in field(" + structField.name() + "): " + fieldObj);
				}
			}
		}

		return new GenericRowWithSchema(
				fields,
				schema);
	}

}
