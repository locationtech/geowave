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

//STANAG 4676
/**
 * information related to the identity of a track
 */
public class TrackIdentity
{
	private Long id;

	/**
	 * identity information about a track.
	 * <p>
	 * values are derived from STANAG 1241. See {@link Identity}
	 */
	public Identity identity;

	/**
	 * identity amplifying/modifying descriptors of a track.
	 * <p>
	 * values are derived from STANAG 1241. The amplification element is filled
	 * only if the identity is not NULL.
	 */
	public IdentityAmplification amplification;

	/**
	 * the source(s) used to determine/estimate the classification
	 */
	public ModalityType source;

	/**
	 * A measure of confidence that a correct identity is made.
	 * <p>
	 * NOTE: This value is set only when the identity is not NULL
	 * <p>
	 * Provides a level of confidence or certainty. Allowed values are 0 to 100,
	 * indicating a percentage of certainty. No guidance is imposed on how this
	 * percentage is calculated, as it will vary depending on the class from
	 * which the enumeration is called. The value 0 indicates no confidence; a
	 * value of 100 indicates the highest possible confidence. This field is
	 * intended to be analogous to credibility (of information) criteria
	 * specified in AJP 2.1, whose values range from 1 to 6, but no assignment
	 * of qualitative confidence statements is imposed on specific ranges of
	 * percentages.
	 */
	public int valueConfidence;

	/**
	 * A measure of reliability of the source used to determine/estimate the
	 * identity
	 * <p>
	 * NOTE: This value is set only when the identity is not NULL
	 * <p>
	 * Provides a measure of confidence in the reliability of the source that
	 * generated the confidence value. Source may be a person, algorithm,
	 * exploitation/tracker system, or unit/organization. Allowed values are 0
	 * to 100. The value 0 indicates no reliability; a value of 100 indicates
	 * the highest possible reliability. This field is intended to be analogous
	 * to reliability (of source) criteria specified in AJP 2.1, whose values
	 * range from A to F, but no assignment of qualitative reliability
	 * statements is imposed on specific ranges of percentages.
	 */
	public int sourceReliability;

	/**
	 * The identification friend foe (IFF) information associated with the
	 * track.
	 */
	public IffMode iffMode;

	/**
	 * The identification friend foe (IFF) information associated with the
	 * track.
	 */
	public String iffValue;

	/**
	 * Name of unit being tracked per STANAG 5527 and AdatP-3.
	 * <p>
	 * Typical example is BFT, where identification of unit being tracked is
	 * well known.
	 */
	public String unitName;

	/**
	 * Symbol of unit being tracked per STANAG 5527 and APP-6A.
	 * <p>
	 * Typical example is BFT, where identification of unit being tracked is
	 * well known.
	 */
	public String unitSymbol;

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	public IdentityAmplification getAmplification() {
		return amplification;
	}

	public void setAmplification(
			IdentityAmplification amplification ) {
		this.amplification = amplification;
	}

	public Identity getIdentity() {
		return identity;
	}

	public void setIdentity(
			Identity identity ) {
		this.identity = identity;
	}

	public ModalityType getSource() {
		return source;
	}

	public void setSource(
			ModalityType source ) {
		this.source = source;
	}

	public int getValueConfidence() {
		return valueConfidence;
	}

	public void setValueConfidence(
			int valueConfidence ) {
		this.valueConfidence = valueConfidence;
	}

	public int getSourceReliability() {
		return sourceReliability;
	}

	public void setSourceReliability(
			int sourceReliability ) {
		this.sourceReliability = sourceReliability;
	}

	public IffMode getIffMode() {
		return iffMode;
	}

	public void setIffMode(
			IffMode iffMode ) {
		this.iffMode = iffMode;
	}

	public String getIffValue() {
		return iffValue;
	}

	public void setIffValue(
			String iffValue ) {
		this.iffValue = iffValue;
	}

	public String getUnitName() {
		return unitName;
	}

	public void setUnitName(
			String unitName ) {
		this.unitName = unitName;
	}

	public String getUnitSymbol() {
		return unitSymbol;
	}

	public void setUnitSymbol(
			String unitSymbol ) {
		this.unitSymbol = unitSymbol;
	}
}
