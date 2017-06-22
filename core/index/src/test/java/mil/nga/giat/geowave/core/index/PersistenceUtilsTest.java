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
package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class PersistenceUtilsTest
{

	public static class APersistable implements
			Persistable
	{

		@Override
		public byte[] toBinary() {
			return new byte[] {
				1,
				2,
				3
			};
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {
			Assert.assertTrue(Arrays.equals(
					bytes,
					new byte[] {
						1,
						2,
						3
					}));

		}

		@Override
		public APersistable getPersistable() {
			return new APersistable();
		}
	}

	@Test
	public void test() {
		APersistable persistable = new APersistable();
		Assert.assertTrue(PersistenceUtils.fromBinary(
				PersistenceUtils.toBinary(new ArrayList<Persistable>())).isEmpty());
		Assert.assertTrue(PersistenceUtils.fromBinary(
				PersistenceUtils.toBinary(Collections.<Persistable> singleton(persistable))).size() == 1);

		Assert.assertTrue(PersistenceUtils.fromBinary(
				PersistenceUtils.toBinary(Arrays.<Persistable> asList(new Persistable[] {
					persistable,
					persistable
				}))).size() == 2);
	}
}
