class geowave::app {

  $geowave_base_app_rpms = [
    "geowave-${geowave::hadoop_vendor_version}-docs",
    "geowave-${geowave::hadoop_vendor_version}-ingest",
  ]

  package { $geowave_base_app_rpms:
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
