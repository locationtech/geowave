package org.locationtech.geowave.datastore.cassandra.cli;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.ArrayList;
import org.apache.commons.exec.CommandLine;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.codehaus.mojo.cassandra.AbstractCassandraMojo;
import org.codehaus.mojo.cassandra.StartCassandraClusterMojo;
import org.codehaus.mojo.cassandra.StartCassandraMojo;
import org.codehaus.mojo.cassandra.StopCassandraClusterMojo;
import org.codehaus.mojo.cassandra.StopCassandraMojo;
import org.locationtech.geowave.core.store.util.ClasspathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraServer.class);

  private final RunCassandraServerOptions options;
  protected static final String NODE_DIRECTORY_PREFIX = "cassandra";

  public CassandraServer(final RunCassandraServerOptions options) {
    this.options = options;
  }
  public CassandraServer(final int numNodes, final int memory, final String directory) {
    this.options = new RunCassandraServerOptions();
    options.setClusterSize(numNodes);
    options.setMemory(memory);
    options.setDirectory(directory);
  }

  public void start() {
    if (options.getClusterSize() > 1) {
      new StartGeoWaveCluster(
          options.getClusterSize(),
          options.getMemory(),
          options.getDirectory()).start();;
    } else {
      new StartGeoWaveStandalone(options.getMemory(), options.getDirectory()).start();
    }
  }

  public void stop() {

  }

  private static class StartGeoWaveCluster extends StartCassandraClusterMojo {
    private StartGeoWaveCluster(final int numNodes, final int memory, final String directory) {
      super();
      startWaitSeconds = 180;
      rpcAddress = "127.0.0.1";
      rpcPort = 9160;
      jmxPort = 7199;
      startNativeTransport = true;
      nativeTransportPort = 9042;
      listenAddress = "127.0.0.1";
      storagePort = 7000;
      stopPort = 8081;
      stopKey = "cassandra-maven-plugin";
      maxMemory = memory;
      cassandraDir = new File(directory, NODE_DIRECTORY_PREFIX);
      logLevel = "ERROR";
      project = new MavenProject();
      project.setFile(cassandraDir);
      Field f;
      try {
        f = StartCassandraClusterMojo.class.getDeclaredField("clusterSize");
        f.setAccessible(true);
        f.set(this, numNodes);
        f = AbstractCassandraMojo.class.getDeclaredField("pluginArtifact");

        f.setAccessible(true);
        final DefaultArtifact a =
            new DefaultArtifact(
                "group",
                "artifact",
                VersionRange.createFromVersionSpec("version"),
                null,
                "type",
                null,
                new DefaultArtifactHandler());
        a.setFile(cassandraDir);
        f.set(this, a);

        f = AbstractCassandraMojo.class.getDeclaredField("pluginDependencies");
        f.setAccessible(true);
        f.set(this, new ArrayList<>());
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
          | IllegalAccessException | InvalidVersionSpecificationException e) {
        LOGGER.error("Unable to initialize start cassandra cluster", e);
      }
    }

    @Override
    protected CommandLine newServiceCommandLine(
        final File cassandraDir,
        final String listenAddress,
        final String rpcAddress,
        final BigInteger initialToken,
        final String[] seeds,
        final boolean jmxRemoteEnabled,
        final int jmxPort) throws IOException {
      return super.newServiceCommandLine(
          cassandraDir,
          listenAddress,
          rpcAddress,
          BigInteger.valueOf(initialToken.longValue()),
          seeds,
          false,
          jmxPort);
    }

    @Override
    protected void createCassandraJar(
        final File jarFile,
        final String mainClass,
        final File cassandraDir) throws IOException {
      ClasspathUtils.setupPathingJarClassPath(
          jarFile,
          mainClass,
          this.getClass(),
          new File(cassandraDir, "conf").toURI().toURL());
    }

    public void start() {
      try {
        super.execute();
      } catch (MojoExecutionException | MojoFailureException e) {
        LOGGER.error("Unable to start cassandra cluster", e);
      }
    }
  }

  private static class StopGeoWaveCluster extends StopCassandraClusterMojo {
    private StopGeoWaveCluster(final int numNodes) {
      super();
      rpcPort = 9160;
      stopPort = 8081;
      stopKey = "cassandra-maven-plugin";
      Field f;
      try {
        f = StopCassandraClusterMojo.class.getDeclaredField("clusterSize");
        f.setAccessible(true);
        f.set(this, numNodes);
        f = StopCassandraClusterMojo.class.getDeclaredField("rpcAddress");

        f.setAccessible(true);
        f.set(this, "127.0.0.1");
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
          | IllegalAccessException e) {
        LOGGER.error("Unable to initialize stop cassandra cluster", e);
      }
    }

    public void stop() {
      try {
        super.execute();
      } catch (MojoExecutionException | MojoFailureException e) {
        LOGGER.error("Unable to stop cassandra cluster", e);
      }
    }
  }

  private static class StartGeoWaveStandalone extends StartCassandraMojo {
    private StartGeoWaveStandalone(final int memory, final String directory) {
      super();
      startWaitSeconds = 180;
      rpcAddress = "127.0.0.1";
      rpcPort = 9160;
      jmxPort = 7199;
      startNativeTransport = true;
      nativeTransportPort = 9042;
      listenAddress = "127.0.0.1";
      storagePort = 7000;
      stopPort = 8081;
      stopKey = "cassandra-maven-plugin";
      maxMemory = memory;
      cassandraDir = new File(directory);
      logLevel = "ERROR";
      project = new MavenProject();
      project.setFile(cassandraDir);
      Field f;
      try {
        f = AbstractCassandraMojo.class.getDeclaredField("pluginArtifact");

        f.setAccessible(true);
        final DefaultArtifact a =
            new DefaultArtifact(
                "group",
                "artifact",
                VersionRange.createFromVersionSpec("version"),
                null,
                "type",
                null,
                new DefaultArtifactHandler());
        a.setFile(cassandraDir);
        f.set(this, a);

        f = AbstractCassandraMojo.class.getDeclaredField("pluginDependencies");
        f.setAccessible(true);
        f.set(this, new ArrayList<>());
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
          | IllegalAccessException | InvalidVersionSpecificationException e) {
        LOGGER.error("Unable to initialize start cassandra cluster", e);
      }
    }

    @Override
    protected void createCassandraJar(
        final File jarFile,
        final String mainClass,
        final File cassandraDir) throws IOException {
      ClasspathUtils.setupPathingJarClassPath(
          jarFile,
          mainClass,
          this.getClass(),
          new File(cassandraDir, "conf").toURI().toURL());
    }

    public void start() {
      try {
        super.execute();
      } catch (MojoExecutionException | MojoFailureException e) {
        LOGGER.error("Unable to start cassandra cluster", e);
      }
    }
  }

  private static class StopGeoWaveStandalone extends StopCassandraMojo {
    public StopGeoWaveStandalone() {
      super();
      rpcPort = 9160;
      stopPort = 8081;
      stopKey = "cassandra-maven-plugin";
      Field f;
      try {
        f = StopCassandraMojo.class.getDeclaredField("rpcAddress");

        f.setAccessible(true);
        f.set(this, "127.0.0.1");
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
          | IllegalAccessException e) {
        LOGGER.error("Unable to initialize stop cassandra cluster", e);
      }
    }

    public void stop() {
      try {
        super.execute();
      } catch (MojoExecutionException | MojoFailureException e) {
        LOGGER.error("Unable to stop cassandra cluster", e);
      }
    }
  }
}
