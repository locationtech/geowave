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

public class NATO4676Message
{
	protected String formatVersion;
	protected long messageTime;
	protected Security security;
	protected IDdata senderID;
	protected Long runId;

	public void setFormatVersion(
			String formatVersion ) {
		this.formatVersion = formatVersion;
	}

	public String getFormatVersion() {
		return formatVersion;
	}

	public long getMessageTime() {
		return messageTime;
	}

	public void setMessageTime(
			long messageTime ) {
		this.messageTime = messageTime;
	}

	public Security getSecurity() {
		return security;
	}

	public void setSecurity(
			Security security ) {
		this.security = security;
	}

	public IDdata getSenderID() {
		return senderID;
	}

	public void setSenderID(
			IDdata senderID ) {
		this.senderID = senderID;
	}

	public Long getRunId() {
		return runId;
	}

	public void setRunId(
			Long id ) {
		this.runId = id;
	}
}
