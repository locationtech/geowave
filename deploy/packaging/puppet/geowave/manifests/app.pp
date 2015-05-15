class geowave::app {

  $geowave_base_app_rpms = [
    "geowave-docs",
    "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-tools",
  ]

  package { $geowave_base_app_rpms:
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
