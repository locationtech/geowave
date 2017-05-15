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
package mil.nga.giat.geowave.adapter.auth;

import java.util.List;

/**
 * Used for Json based authorization data sets.
 * 
 * 
 */
public class AuthorizationEntry
{
	String userName;
	List<String> authorizations;

	protected String getUserName() {
		return userName;
	}

	protected void setUserName(
			String userName ) {
		this.userName = userName;
	}

	protected List<String> getAuthorizations() {
		return authorizations;
	}

	protected void setAuthorizations(
			List<String> authorizations ) {
		this.authorizations = authorizations;
	}

}
