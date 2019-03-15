package org.locationtech.geowave.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import com.jcraft.jsch.Logger;

public class KuduLocal {

  // Tracking the cloudera precompiled package
  // https://www.cloudera.com/documentation/enterprise/5-16-x/topics/cdh_ig_yumrepo_local_create.html#topic_30__section_sl2_xdw_wm

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(KuduLocal.class);
  private static final String KUDU_REPO_URL =
      "https://archive.cloudera.com/cdh5/ubuntu/xenial/amd64/cdh/pool/contrib/k/kudu/";
  private static final String KUDU_DEB_PACKAGE =
      "kudu_1.7.0+cdh5.16.1+0-1.cdh5.16.1.p0.3~xenial-cdh5.16.1_amd64.deb";
  private static final String KUDU_MASTER = "kudu-master";
  private static final String KUDU_TABLET = "kudu-tserver";
  private static final long STARTUP_DELAY_MS = 1000L;

  private final int numTablets;
  private final File kuduLocalDir;

  private final ExecuteWatchdog watchdog;
  private final Executor executor = new DefaultExecutor();

  public KuduLocal(String localDir, int numTablets) {
    if (TestUtils.isSet(localDir)) {
      this.kuduLocalDir = new File(localDir);
    } else {
      this.kuduLocalDir = new File(TestUtils.TEMP_DIR, "kudu");
    }
    if (!this.kuduLocalDir.exists() && !this.kuduLocalDir.mkdirs()) {
      LOGGER.error("unable to create directory {}", this.kuduLocalDir.getAbsolutePath());
    } else if (!this.kuduLocalDir.isDirectory()) {
      LOGGER.error("{} exists but is not a directory", this.kuduLocalDir.getAbsolutePath());
    }
    this.numTablets = numTablets;

    this.watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(this.watchdog);
    executor.setWorkingDirectory(this.kuduLocalDir);
  }

  public boolean start() {
    if (!isInstalled()) {
      try {
        if (!install()) {
          return false;
        }
      } catch (IOException | ArchiveException e) {
        LOGGER.error("Kudu installation error: {}", e.getMessage());
        return false;
      }
    }

    try {
      startKuduLocal();
    } catch (IOException | InterruptedException e) {
      LOGGER.error("Kudu start error: {}", e.getMessage());
      return false;
    }

    return true;
  }

  public boolean isRunning() {
    return (watchdog != null && watchdog.isWatching());
  }

  public void stop() {
    watchdog.destroyProcess();
  }

  private boolean isInstalled() {
    File kuduMasterBinary = new File(kuduLocalDir, KUDU_MASTER);
    File kuduTabletBinary = new File(kuduLocalDir, KUDU_TABLET);
    boolean okMaster = kuduMasterBinary.exists() && kuduMasterBinary.canExecute();
    boolean okTablet = kuduTabletBinary.exists() && kuduTabletBinary.canExecute();
    return okMaster && okTablet;
  }

  private boolean install() throws IOException, ArchiveException {
    LOGGER.info("Installing {}", KUDU_DEB_PACKAGE);

    LOGGER.debug("downloading kudu debian package");
    File debPackageFile = new File(kuduLocalDir, KUDU_DEB_PACKAGE);
    if (!debPackageFile.exists()) {
      HttpURLConnection.setFollowRedirects(true);
      URL url = new URL(KUDU_REPO_URL + KUDU_DEB_PACKAGE);
      try (FileOutputStream fos = new FileOutputStream(debPackageFile)) {
        IOUtils.copy(url.openStream(), fos);
        fos.flush();
      }
    }

    LOGGER.debug("extracting kudu debian package data contents");
    File debDataTarGz = new File(kuduLocalDir, "data.tar.gz");
    if (!debDataTarGz.exists()) {
      try (FileInputStream fis = new FileInputStream(debPackageFile);
          ArchiveInputStream debInputStream =
              new ArchiveStreamFactory().createArchiveInputStream("ar", fis)) {
        ArchiveEntry entry = null;
        while ((entry = debInputStream.getNextEntry()) != null) {
          if (debDataTarGz.getName().equals(entry.getName())) {
            try (FileOutputStream fos = new FileOutputStream(debDataTarGz)) {
              IOUtils.copy(debInputStream, fos);
            }
            break;
          }
        }
      }
    }

    LOGGER.debug("extracting kudu data contents");
    TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
    unarchiver.enableLogging(new ConsoleLogger(Logger.WARN, "Kudu Local Unarchive"));
    unarchiver.setSourceFile(debDataTarGz);
    unarchiver.setDestDirectory(kuduLocalDir);
    unarchiver.extract();

    for (File f : new File[] {debPackageFile, debDataTarGz}) {
      if (!f.delete()) {
        LOGGER.warn("cannot delete " + f.getAbsolutePath());
      }
    }

    LOGGER.debug("moving kudu master and tablet binaries to {}", kuduLocalDir);
    // move the master and tablet server binaries into the kudu local directory
    Path kuduBin = Paths.get(kuduLocalDir.getAbsolutePath(), "usr", "lib", "kudu", "sbin-release");
    File kuduMasterBinary = kuduBin.resolve(KUDU_MASTER).toFile();
    File kuduTabletBinary = kuduBin.resolve(KUDU_TABLET).toFile();
    kuduMasterBinary.setExecutable(true);
    kuduTabletBinary.setExecutable(true);
    FileUtils.moveFileToDirectory(kuduMasterBinary, kuduLocalDir, false);
    FileUtils.moveFileToDirectory(kuduTabletBinary, kuduLocalDir, false);

    if (isInstalled()) {
      LOGGER.info("Kudu Local installation successful");
      return true;
    } else {
      LOGGER.error("Kudu Local installation failed");
      return false;
    }
  }

  private void startKuduLocal() throws ExecuteException, IOException, InterruptedException {
    // Using a result handler makes the local instance run async
    DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

    File kuduMasterBinary = new File(kuduLocalDir.getAbsolutePath(), KUDU_MASTER);
    CommandLine startMaster = new CommandLine(kuduMasterBinary.getAbsolutePath());
    startMaster.addArgument("--fs_data_dirs");
    startMaster.addArgument(new File(kuduLocalDir, "master_fs_data").getAbsolutePath());
    startMaster.addArgument("--fs_metadata_dir");
    startMaster.addArgument(new File(kuduLocalDir, "master_fs_metadata").getAbsolutePath());
    startMaster.addArgument("--fs_wal_dir");
    startMaster.addArgument(new File(kuduLocalDir, "master_fs_wal").getAbsolutePath());
    executor.execute(startMaster, resultHandler);

    File kuduTabletBinary = new File(kuduLocalDir.getAbsolutePath(), KUDU_TABLET);
    for (int i = 0; i < numTablets; i++) {
      CommandLine startTablet = new CommandLine(kuduTabletBinary.getAbsolutePath());
      startTablet.addArgument("--fs_data_dirs");
      startTablet.addArgument(new File(kuduLocalDir, "t" + i + "_fs_data_").getAbsolutePath());
      startTablet.addArgument("--fs_metadata_dir");
      startTablet.addArgument(new File(kuduLocalDir, "t" + i + "_fs_metadata").getAbsolutePath());
      startTablet.addArgument("--fs_wal_dir");
      startTablet.addArgument(new File(kuduLocalDir, "t" + i + "_fs_wal").getAbsolutePath());
      executor.execute(startTablet, resultHandler);
    }

    Thread.sleep(STARTUP_DELAY_MS);
  }

  public static void main(String[] args) {
    KuduLocal kudu = new KuduLocal(null, 1);
    kudu.start();
  }

}
