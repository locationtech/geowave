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

public class Security
{
	private ClassificationLevel classification;
	private String policyName;
	private String controlSystem;
	private String dissemination;
	private String releasability;

	public ClassificationLevel getClassification() {
		return classification;
	}

	public void setClassification(
			ClassificationLevel classification ) {
		this.classification = classification;
	}

	public String getPolicyName() {
		return policyName;
	}

	public void setPolicyName(
			String policyName ) {
		this.policyName = policyName;
	}

	public String getControlSystem() {
		return controlSystem;
	}

	public void setControlSystem(
			String controlSystem ) {
		this.controlSystem = controlSystem;
	}

	public String getDissemination() {
		return dissemination;
	}

	public void setDissemination(
			String dissemination ) {
		this.dissemination = dissemination;
	}

	public String getReleasability() {
		return releasability;
	}

	public void setReleasability(
			String releasability ) {
		this.releasability = releasability;
	}

}
