class geowave::gwtomcat8_service {
  service { 'gwtomcat8':
    ensure   => 'running',
    provider => 'redhat',
    enable   => true,
  }
}
