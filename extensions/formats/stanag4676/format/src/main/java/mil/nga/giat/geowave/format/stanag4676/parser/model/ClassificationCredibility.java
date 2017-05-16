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

public class ClassificationCredibility
{
	/**
	 * A measure of confidence that a correct object classification is made.
	 * <p>
	 * NOTE: This value is set only when the classification is not UNKNOWN
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
	private Integer valueConfidence;

	/**
	 * A measure of reliability of the source used to determine/estimate the
	 * classification
	 * <p>
	 * NOTE: This value is set only when the classification is not UNKNOWN
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
	private Integer sourceReliability;

	public ClassificationCredibility() {}

	public ClassificationCredibility(
			Integer valueConfidence,
			Integer sourceReliability ) {
		this.valueConfidence = valueConfidence;
		this.sourceReliability = sourceReliability;
	}

	public Integer getValueConfidence() {
		return valueConfidence;
	}

	public void setValueConfidence(
			Integer valueConfidence ) {
		this.valueConfidence = valueConfidence;
	}

	public Integer getSourceReliability() {
		return sourceReliability;
	}

	public void setSourceReliability(
			Integer sourceReliability ) {
		this.sourceReliability = sourceReliability;
	}
}
