class geowave::accumulo {

  package { "geowave-${geowave::hadoop_vendor_version}-accumulo":
    ensure => latest,
    tag    => 'geowave-package',
  }

  if !defined(Package["geowave-${geowave::hadoop_vendor_version}-core"]) {
    package { "geowave-${geowave::hadoop_vendor_version}-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

}
