class geowave::gwtomcat8_service {
  service { 'gwtomcat8':
    ensure     => 'running',
    enable     => true,
    hasstatus  => true,
  }
}
