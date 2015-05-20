class geowave::accumulo {

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-accumulo":
    ensure => latest,
    tag    => 'geowave-package',
  }

  if !defined(Package["geowave-core"]) {
    package { "geowave-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

}
