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
package mil.nga.giat.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;
import java.util.LinkedList;
import java.util.UUID;

public class MemoryTransactionsAllocator implements
		TransactionsAllocator
{
	private final LinkedList<String> lockPaths = new LinkedList<String>();

	public MemoryTransactionsAllocator() {
		super();
	}

	public void close()
			throws InterruptedException {}

	@Override
	public void releaseTransaction(
			final String txID )
			throws IOException {
		synchronized (lockPaths) {
			if (!lockPaths.contains(txID)) {
				lockPaths.add(txID);
			}
		}

	}

	@Override
	public String getTransaction()
			throws IOException {
		synchronized (lockPaths) {
			if (lockPaths.size() > 0) {
				return lockPaths.removeFirst();
			}
		}
		return UUID.randomUUID().toString();
	}

}
