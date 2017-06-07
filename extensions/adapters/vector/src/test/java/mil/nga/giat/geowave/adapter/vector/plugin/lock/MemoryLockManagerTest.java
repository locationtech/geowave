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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.plugin.lock.LockingManagement;
import mil.nga.giat.geowave.adapter.vector.plugin.lock.MemoryLockManager;

import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureLock;
import org.geotools.data.Transaction;
import org.junit.Test;

public class MemoryLockManagerTest
{

	@Test
	public void testRelockLock()
			throws InterruptedException,
			IOException {
		final LockingManagement memoryLockManager = new MemoryLockManager(
				"default");
		final DefaultTransaction t1 = new DefaultTransaction();
		memoryLockManager.lock(
				t1,
				"f8");
		memoryLockManager.lock(
				t1,
				"f8");
		t1.commit();
		t1.close();
	}

	@Test
	public void testLockWithProperAuth()
			throws InterruptedException,
			IOException {
		final LockingManagement memoryLockManager = new MemoryLockManager(
				"default");
		final Transaction t1 = Transaction.AUTO_COMMIT;
		final DefaultTransaction t2 = new DefaultTransaction();
		t2.addAuthorization("auth5");
		FeatureLock lock = new FeatureLock(
				"auth5",
				1 /* minute */);
		memoryLockManager.lockFeatureID(
				"sometime",
				"f5",
				t1,
				lock);
		Thread commiter = new Thread(
				new Runnable() {
					@Override
					public void run() {
						try {
							Thread.sleep(4000);
							memoryLockManager.release(
									"auth5",
									t1);
						}
						catch (InterruptedException e) {
							e.printStackTrace();
							throw new RuntimeException(
									e);
						}
						catch (IOException e) {
							e.printStackTrace();
							throw new RuntimeException(
									e);
						}
					}
				});
		long currentTime = System.currentTimeMillis();
		commiter.start();
		memoryLockManager.lock(
				t2,
				"f5");
		assertTrue((System.currentTimeMillis() - currentTime) < 4000);
		commiter.join();
	}

	@Test
	public void testLockReleaseOfBulkAuthLock()
			throws InterruptedException,
			IOException {
		final LockingManagement memoryLockManager = new MemoryLockManager(
				"default");
		final Transaction t1 = Transaction.AUTO_COMMIT;
		final DefaultTransaction t2 = new DefaultTransaction();
		t2.addAuthorization("auth1");
		FeatureLock lock = new FeatureLock(
				"auth1",
				1 /* minute */);
		memoryLockManager.lockFeatureID(
				"sometime",
				"f4",
				t1,
				lock);
		memoryLockManager.lock(
				t2,
				"f4");
		t2.commit();
		// commit should not take away the lock
		assertTrue(memoryLockManager.exists("auth1"));
		memoryLockManager.release(
				"auth1",
				t1);
		assertFalse(memoryLockManager.exists("auth1"));
		t1.close();
	}

	@Test
	public void testReset()
			throws InterruptedException,
			IOException {
		final LockingManagement memoryLockManager = new MemoryLockManager(
				"default");
		final Transaction t1 = Transaction.AUTO_COMMIT;
		FeatureLock lock = new FeatureLock(
				"auth2",
				1 /* minute */);
		memoryLockManager.lockFeatureID(
				"sometime",
				"f2",
				t1,
				lock);
		memoryLockManager.refresh(
				"auth2",
				t1);
		assertTrue(memoryLockManager.exists("auth2"));
		memoryLockManager.release(
				"auth2",
				t1);
		assertFalse(memoryLockManager.exists("auth2"));
	}

	@Test
	public void testBlockinLock()
			throws InterruptedException,
			IOException {
		final LockingManagement memoryLockManager = new MemoryLockManager(
				UUID.randomUUID().toString());
		final DefaultTransaction t1 = new DefaultTransaction();
		memoryLockManager.lock(
				t1,
				"f3");
		final DefaultTransaction t2 = new DefaultTransaction();

		Thread commiter = new Thread(
				new Runnable() {
					@Override
					public void run() {
						try {
							Thread.sleep(4000);
							// System.out.println("commit");
							t1.commit();
						}
						catch (InterruptedException e) {
							e.printStackTrace();
							throw new RuntimeException(
									e);
						}
						catch (IOException e) {
							e.printStackTrace();
							throw new RuntimeException(
									e);
						}
					}
				});

		long currentTime = System.currentTimeMillis();
		commiter.start();
		// will block\
		// System.out.println("t2");
		memoryLockManager.lock(
				t2,
				"f3");
		final long endTime = System.currentTimeMillis();
		// System.out.println(endTime + " > " + currentTime);
		assertTrue((endTime - currentTime) >= 3800);

		commiter.join();
		t2.commit();
		t2.close();
		t1.close();

	}
}
