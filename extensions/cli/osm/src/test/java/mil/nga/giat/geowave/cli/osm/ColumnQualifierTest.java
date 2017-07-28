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
package mil.nga.giat.geowave.cli.osm;

import com.google.common.io.BaseEncoding;

import mil.nga.giat.geowave.cli.osm.accumulo.osmschema.Schema;

import org.junit.Assert;
import org.junit.Test;

public class ColumnQualifierTest
{

	@Test(expected = NullPointerException.class)
	public void TAG_QUALIFIER_NULL()
			throws Exception {
		byte[] bytes = Schema.CQ.TAG_QUALIFIER(null);
		if (bytes != null) {
			Assert.fail("returned non null value back; execution path should never be seen");
		}
	}

	@Test
	public void TAG_QUALIFIER_TOBYTE()
			throws Exception {
		byte[] bytes = Schema.CQ.TAG_QUALIFIER("フォースを使え　ルーク");
		byte[] bytes2 = BaseEncoding.base64().decode(
				"44OV44Kp44O844K544KS5L2/44GI44CA44Or44O844Kv");
		Assert.assertArrayEquals(
				bytes,
				bytes2);
	}
}
