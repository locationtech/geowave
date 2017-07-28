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
package mil.nga.giat.geowave.core.index.persist;

import java.util.function.Supplier;

public interface PersistableRegistrySpi
{

	public PersistableIdAndConstructor[] getSupportedPersistables();

	public static class PersistableIdAndConstructor
	{
		private final short persistableId;
		private final Supplier<Persistable> persistableConstructor;

		public PersistableIdAndConstructor(
				final short persistableId,
				final Supplier<Persistable> persistableConstructor ) {
			this.persistableId = persistableId;
			this.persistableConstructor = persistableConstructor;
		}

		public short getPersistableId() {
			return persistableId;
		}

		public Supplier<Persistable> getPersistableConstructor() {
			return persistableConstructor;
		}
	}

}
