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
package org.locationtech.geowave.analytic.spark.sparksql.udt;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;

/**
 * Created by jwileczek on 7/20/18.
 */
public abstract class AbstractGeometryUDT<T extends Geometry> extends
		UserDefinedType<T>
{
	@Override
	public DataType sqlType() {
		return new StructType(
				new StructField[] {
					new StructField(
							"wkb",
							DataTypes.BinaryType,
							true,
							Metadata.empty())
				});
	}

	@Override
	public String pyUDT() {
		return "geowave_pyspark.types." + this.getClass().getSimpleName();
	}

	@Override
	public InternalRow serialize(
			T obj ) {
		byte[] bytes = new WKBWriter().write(obj);
		InternalRow returnRow = new GenericInternalRow(
				bytes.length);
		returnRow.update(
				0,
				bytes);
		return returnRow;
	}

	@Override
	public T deserialize(
			Object datum ) {
		T geom = null;
		InternalRow row = (InternalRow) datum;
		byte[] bytes = row.getBinary(0);
		try {
			geom = (T) new WKBReader().read(bytes);
		}
		catch (ParseException e) {
			e.printStackTrace();
		}
		return geom;
	}

}
