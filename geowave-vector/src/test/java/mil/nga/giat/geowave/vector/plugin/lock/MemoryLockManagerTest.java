package mil.nga.giat.geowave.vector.plugin.lock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import mil.nga.giat.geowave.vector.plugin.lock.LockingManagement;
import mil.nga.giat.geowave.vector.plugin.lock.MemoryLockManager;

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
				"f1");
		memoryLockManager.lock(
				t1,
				"f1");
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
		t2.addAuthorization("auth1");
		FeatureLock lock = new FeatureLock(
				"auth1",
				1 /* minute */);
		memoryLockManager.lockFeatureID(
				"sometime",
				"f1",
				t1,
				lock);
		Thread commiter = new Thread(
				new Runnable() {
					@Override
					public void run() {
						try {
							Thread.sleep(4000);
							memoryLockManager.release(
									"auth1",
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
				"f1");
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
				"f1",
				t1,
				lock);
		memoryLockManager.lock(
				t2,
				"f1");
		t2.commit();
		// commit should not take away the lock
		assertTrue(memoryLockManager.exists("auth1"));
		memoryLockManager.release(
				"auth1",
				t1);
		assertFalse(memoryLockManager.exists("auth1"));
	}

	@Test
	public void testReset()
			throws InterruptedException,
			IOException {
		final LockingManagement memoryLockManager = new MemoryLockManager(
				"default");
		final Transaction t1 = Transaction.AUTO_COMMIT;
		FeatureLock lock = new FeatureLock(
				"auth1",
				1 /* minute */);
		memoryLockManager.lockFeatureID(
				"sometime",
				"f1",
				t1,
				lock);
		memoryLockManager.refresh(
				"auth1",
				t1);
		assertTrue(memoryLockManager.exists("auth1"));
		memoryLockManager.release(
				"auth1",
				t1);
		assertFalse(memoryLockManager.exists("auth1"));
	}

	@Test
	public void testBlockinLock()
			throws InterruptedException,
			IOException {
		final LockingManagement memoryLockManager = new MemoryLockManager(
				"default");
		final DefaultTransaction t1 = new DefaultTransaction();
		memoryLockManager.lock(
				t1,
				"f1");
		final DefaultTransaction t2 = new DefaultTransaction();

		Thread commiter = new Thread(
				new Runnable() {
					@Override
					public void run() {
						try {
							Thread.sleep(4000);
					//		System.out.println("commit");
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
	//	System.out.println("t2");
		memoryLockManager.lock(
				t2,
				"f1");
		assertTrue((System.currentTimeMillis() - currentTime) >= 4000);

		commiter.join();
		t2.commit();
		t2.close();
		t1.close();

	}
}
