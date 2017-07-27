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

//STANAG 4676
/**
 * Associates related and possibly related tracks to each other. Often there is
 * ambiguity as to whether two tracks are actually the same object.
 * Additionally, multiple objects may converge to appear as a single object or,
 * multiple objects may split from a single track to multiple tracks. The
 * LineageRelation allows all track segments which may be interconnected or
 * related to be identified.
 */
public class LineageRelation
{
	private Long id;
	/**
	 * The UUID of the LineageRelation
	 */
	public UUID uuid;

	/**
	 * The track number of a separate track that is related to the reported
	 * track.
	 */
	public String relatedTrackNumber;

	/**
	 * The UUID of a separate track that is related to the reported track.
	 */
	public UUID relatedTrackUuid;

	/**
	 * The relationship between a separate track and the reported track.
	 */
	public LineageRelationType relation;

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(
			UUID uuid ) {
		this.uuid = uuid;
	}

	public String getRelatedTrackNumber() {
		return relatedTrackNumber;
	}

	public void setRelatedTrackNumber(
			String relatedTrackNumber ) {
		this.relatedTrackNumber = relatedTrackNumber;
	}

	public UUID getRelatedTrackUuid() {
		return relatedTrackUuid;
	}

	public void setRelatedTrackUuid(
			UUID relatedTrackUuid ) {
		this.relatedTrackUuid = relatedTrackUuid;
	}

	public LineageRelationType getRelation() {
		return relation;
	}

	public void setRelation(
			LineageRelationType relation ) {
		this.relation = relation;
	}

}
