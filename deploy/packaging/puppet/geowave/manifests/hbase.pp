class geowave::hbase {

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-hbase":
    ensure => latest,
    tag    => 'geowave-package',
  }

  if !defined(Package["geowave-${geowave::geowave_version}-core"]) {
    package { "geowave-${geowave::geowave_version}-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

}
