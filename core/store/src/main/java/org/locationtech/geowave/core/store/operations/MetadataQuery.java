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
package org.locationtech.geowave.core.store.operations;

public class MetadataQuery
{
	private final byte[] primaryId;
	private final byte[] secondaryId;
	private final String[] authorizations;

	public MetadataQuery(
			final byte[] primaryId,
			final byte[] secondaryId,
			final String... authorizations ) {
		this.primaryId = primaryId;
		this.secondaryId = secondaryId;
		this.authorizations = authorizations;
	}

	public byte[] getPrimaryId() {
		return primaryId;
	}

	public byte[] getSecondaryId() {
		return secondaryId;
	}

	public boolean hasPrimaryId() {
		return (primaryId != null) && (primaryId.length > 0);
	}

	public boolean hasSecondaryId() {
		return (secondaryId != null) && (secondaryId.length > 0);
	}

	public String[] getAuthorizations() {
		return authorizations;
	}

}
