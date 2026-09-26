## GeoWave REST Web-App

* Capabilities
  * A Jakarta REST (Jersey 3.1) web application that exposes every GeoWave command implementing `ServiceEnabledCommand` as an API endpoint under `/v0`.
  * The API is generated at startup and described in Swagger 2.0 at `/api`. The main page (`/`) lists the routes.
* Building and deploying
  * `mvn package -P rest-services-war` builds `target/geowave-service-rest-<version>-restservices.war`, which runs on a Servlet 6 container such as Tomcat 10.1 or 11, or Jetty 12 (ee10 or ee11).
  * The init parameters in `WEB-INF/web.xml` set the GeoWave config file (`config_file`), the host for the Swagger description (`host_port`) and the API key database (`api_key_db`).
* Security
  * Authentication is left to the servlet container or to a reverse proxy in front of it, for example a `security-constraint` and `login-config` in `web.xml`; the file has a commented example.
  * API keys: with `api_key_db` set to a SQLite database file, every request under `/v0` needs a valid `apiKey` query parameter and is otherwise rejected with 401. A user the container has authenticated is given a key the first time they are seen, and sees it on the main page.
  * Up to GeoWave 2.x this was a Restlet application secured with Spring Security, with an OAuth2 (Facebook) example configuration. Spring Security is no longer used; see the migration notes in the user guide.
