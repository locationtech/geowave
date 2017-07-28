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
package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.index.persist.PersistableRegistrySpi;
import mil.nga.giat.geowave.datastore.hbase.server.MergingServerOp;
import mil.nga.giat.geowave.datastore.hbase.server.MergingVisibilityServerOp;
import mil.nga.giat.geowave.datastore.hbase.server.RowMergingServerOp;
import mil.nga.giat.geowave.datastore.hbase.server.RowMergingVisibilityServerOp;

public class HBasePersistableRegistry implements
		PersistableRegistrySpi
{

	@Override
	public PersistableIdAndConstructor[] getSupportedPersistables() {
		return new PersistableIdAndConstructor[] {
			new PersistableIdAndConstructor(
					(short) 1600,
					MergingServerOp::new),
			new PersistableIdAndConstructor(
					(short) 1601,
					MergingVisibilityServerOp::new),
			new PersistableIdAndConstructor(
					(short) 1602,
					RowMergingServerOp::new),
			new PersistableIdAndConstructor(
					(short) 1603,
					RowMergingVisibilityServerOp::new),
		};
	}
}
