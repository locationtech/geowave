# Security Policy

LocationTech GeoWave is an Eclipse Foundation project, and adheres to the
[Eclipse Foundation Vulnerability Reporting Policy](https://www.eclipse.org/security/policy/).

## How To Report a Vulnerability

If you think you have found a vulnerability in this repository, please report it to us through
coordinated disclosure.

**Please do not report security vulnerabilities through public issues, discussions, or pull
requests.**

Instead, report it using one of the following ways:

* Create a [confidential issue](https://gitlab.eclipse.org/security/vulnerability-reports/-/work_items/new?issue[confidential]=true)
  in the Eclipse Foundation Vulnerability Reporting Tracker
* Report a [vulnerability](https://github.com/locationtech/geowave/security/advisories/new)
  directly via private vulnerability reporting on GitHub

You can also email the Eclipse Foundation Security Team at
[security@eclipse-foundation.org](mailto:security@eclipse-foundation.org). More information about
reporting and disclosure is on the [Eclipse Foundation Security page](https://www.eclipse.org/security/).

Please include as much of the following as you can, to help us understand and resolve the issue:

* The type of issue
* The affected GeoWave version(s), and the component or data store involved
* The impact of the issue, including how an attacker might exploit it
* Step-by-step instructions to reproduce it, and any configuration they need
* The location of the affected source code (tag, branch, commit or URL)
* Related log files, if possible
* Proof-of-concept or exploit code, if possible

## Supported Versions

Development, including security fixes, happens on `master`, the 3.x line, which requires Java 21.
`2.x-jdk8` is the last line that runs on Java 8.
