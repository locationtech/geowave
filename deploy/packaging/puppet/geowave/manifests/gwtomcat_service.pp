class geowave::gwtomcat_service {
  service { 'gwtomcat':
    ensure   => 'running',
    provider => 'redhat',
    enable   => true,
  }
}
