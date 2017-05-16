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
package mil.nga.giat.geowave.adapter.vector.plugin.lock;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.geotools.data.Transaction;
import org.geotools.data.Transaction.State;

/**
 * Instances of this class represent a the lock constraints associated with one
 * or more feature instances.
 * 
 * 
 * When serializing this object, note the reserialization requires setting the
 * lockingManagement attribute.
 * 
 * 
 */
@SuppressFBWarnings({
	"SE_TRANSIENT_FIELD_NOT_RESTORED"
})
public class AuthorizedLock implements
		State,
		java.io.Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1421146354351269795L;

	private Set<String> authorizations = new HashSet<String>();
	private String ID = UUID.randomUUID().toString();
	private long expireTime = System.currentTimeMillis();
	private transient AbstractLockingManagement lockingManagement;
	private long expiryInMinutes;

	public AuthorizedLock() {

	}

	public AuthorizedLock(
			AbstractLockingManagement lockingManagement,
			long expiryInMinutes ) {
		super();
		expireTime = System.currentTimeMillis() + expiryInMinutes * 60000;
		this.expiryInMinutes = expiryInMinutes;
		this.lockingManagement = lockingManagement;
	}

	public AuthorizedLock(
			AbstractLockingManagement lockingManagement,
			String authorization,
			long expiryInMinutes ) {
		super();
		this.authorizations.add(authorization);
		expireTime = System.currentTimeMillis() + expiryInMinutes * 60000;
		this.expiryInMinutes = expiryInMinutes;
		this.lockingManagement = lockingManagement;
	}

	public AuthorizedLock(
			AbstractLockingManagement lockingManagement,
			Set<String> authorizations,
			long expiryInMinutes ) {
		super();
		this.authorizations.addAll(authorizations);
		expireTime = System.currentTimeMillis() + expiryInMinutes * 60000;
		this.expiryInMinutes = expiryInMinutes;
		this.lockingManagement = lockingManagement;
	}

	public AbstractLockingManagement getLockingManagement() {
		return lockingManagement;
	}

	public void setLockingManagement(
			AbstractLockingManagement lockingManagement ) {
		this.lockingManagement = lockingManagement;
	}

	public void resetExpireTime() {
		expireTime = System.currentTimeMillis() + expiryInMinutes * 60000;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ID == null) ? 0 : ID.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		AuthorizedLock other = (AuthorizedLock) obj;
		if (ID == null) {
			if (other.ID != null) return false;
		}
		else if (!ID.equals(other.ID)) return false;
		return true;
	}

	public long getExpireTime() {
		return expireTime;
	}

	public boolean isStale() {
		return expireTime < System.currentTimeMillis();
	}

	@Override
	public synchronized void setTransaction(
			Transaction transaction ) {
		if (transaction != null) {
			this.resetExpireTime();
			this.authorizations.addAll(transaction.getAuthorizations());
		}
	}

	@Override
	public synchronized void addAuthorization(
			String AuthID )
			throws IOException {
		authorizations.add(AuthID);
	}

	public synchronized void invalidate() {
		this.expireTime = 0;
		this.notify();
	}

	public boolean isAuthorized(
			AuthorizedLock lock ) {
		boolean ok = false;
		for (String auth : lock.authorizations) {
			ok |= isAuthorized(auth);
		}
		return ok || this.ID.equals(lock.ID);
	}

	public boolean isAuthorized(
			String authID ) {
		return this.authorizations.contains(authID);
	}

	@Override
	public synchronized void commit()
			throws IOException {
		this.authorizations.clear(); // need to remove authorizations to release
										// only those
		// locks that this transaction created (same ID)
		lockingManagement.releaseAll(this);
		invalidate();
	}

	@Override
	public synchronized void rollback() {
		this.authorizations.clear(); // need to remove authorizations to release
										// only those
		// locks that this transaction created (same ID)
		lockingManagement.releaseAll(this);
		invalidate();
	}
}
