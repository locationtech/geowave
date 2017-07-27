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
package mil.nga.giat.geowave.core.store.adapter.statistics.histogram;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.math.BigDecimal;
import java.math.BigInteger;

public class ByteUtils
{

	private static final byte[] INFINITY_BYTE = new byte[] {
		(byte) 0xff,
		(byte) 0xff,
		(byte) 0xff,
		(byte) 0xff,
		(byte) 0xff,
		(byte) 0xff,
		(byte) 0xff,
		(byte) 0xff
	};

	public static byte[] toBytes(
			double val ) {
		BigInteger tmp = new BigDecimal(
				val).toBigInteger();
		byte[] arr = tmp.toByteArray();
		if (arr[0] == (byte) 0 && arr.length > 1 && arr[1] == (byte) 0xff) {
			// to represent {0xff, 0xff}, big integer uses {0x00, 0xff, 0xff}
			// due to the one's compliment representation.
			byte[] clipped = new byte[arr.length - 1];
			System.arraycopy(
					arr,
					1,
					clipped,
					0,
					arr.length - 1);
			arr = clipped;
		}
		if (arr.length > 8) {
			arr = INFINITY_BYTE;
		}
		return toPaddedBytes(arr);
	}

	public static byte[] toBytes(
			long val ) {
		byte[] arr = BigInteger.valueOf(
				val).toByteArray();
		if (arr[0] == (byte) 0 && arr.length > 1 && arr[1] == (byte) 0xff) {
			// to represent {0xff, 0xff}, big integer uses {0x00, 0xff, 0xff}
			// due to the one's compliment representation.
			byte[] clipped = new byte[arr.length - 1];
			System.arraycopy(
					arr,
					1,
					clipped,
					0,
					arr.length - 1);
			arr = clipped;
		}
		if (arr.length > 8) {
			arr = INFINITY_BYTE;
		}
		return toPaddedBytes(arr);
	}

	public static long toLong(
			final byte[] data ) {
		return new BigInteger(
				toPaddedBytes(data)).longValue();
	}

	public static double toDouble(
			final byte[] data ) {
		return new BigInteger(
				toPaddedBytes(data)).doubleValue();
	}

	public static byte[] toPaddedBytes(
			final byte[] b ) {
		if (b.length == 8) return b;
		final byte[] newD = new byte[8];
		System.arraycopy(
				b,
				0,
				newD,
				0,
				Math.min(
						b.length,
						8));
		return newD;
	}

}
