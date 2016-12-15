class geowave::service {

  service { 'geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}':
    ensure     => 'running',
    enable     => true,
    hasstatus  => true,
    hasrestart => true,
  }

}
