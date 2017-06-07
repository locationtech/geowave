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
package mil.nga.giat.geowave.format.stanag4676.parser.model;

import java.util.UUID;

public class TrackItem
{
	private UUID uuid;
	private Security security;
	private long time;
	private String source;
	private String comment;

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(
			UUID uuid ) {
		this.uuid = uuid;
	}

	public Security getSecurity() {
		return security;
	}

	public void setSecurity(
			Security security ) {
		this.security = security;
	}

	public long getTime() {
		return time;
	}

	public void setTime(
			long time ) {
		this.time = time;
	}

	public String getSource() {
		return source;
	}

	public void setSource(
			String source ) {
		this.source = source;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(
			String comment ) {
		this.comment = comment;
	}

}
