# install

build plugin:

```
mvn clean install -DskipTests
```

unzip folder and copy to trino plugin folder

```
cp -R trino-simple-db-logger-370 /path/to/trino/folder/plugin
```

create schema

```
create database trino_db_event_listener;

create `event-listener.properties` file:

```
event-listener.name=db-event-listener
db-listener.url=jdbc:mysql://localhost:3306/trino_db_event_listener
db-listener.user=trino
db-listener.password=trino
```

restart trino

verify plugin is loaded and table got created in mysql

if working correctly, will see the following in the Trino `server.log` file:

```
2022-04-29T16:03:11.763-0400	INFO	main	io.trino.eventlistener.EventListenerManager	-- Loading event listener etc/event-listener.properties --
2022-04-29T16:03:11.919-0400	INFO	main	io.trino.dblistener.FlywayMigration	Performing migrations...
2022-04-29T16:03:12.235-0400	INFO	main	org.flywaydb.core.internal.license.VersionPrinter	Flyway Community Edition 7.15.0 by Redgate
2022-04-29T16:03:12.236-0400	INFO	main	org.flywaydb.core.internal.database.base.BaseDatabaseType	Database: jdbc:mysql://localhost:3306/bell_logger (MySQL 8.0)
2022-04-29T16:03:12.406-0400	INFO	main	org.flywaydb.core.internal.command.DbValidate	Successfully validated 1 migration (execution time 00:00.024s)
2022-04-29T16:03:12.440-0400	INFO	main	org.flywaydb.core.internal.schemahistory.JdbcTableSchemaHistory	Creating Schema History table `bell_logger`.`flyway_schema_history` ...
2022-04-29T16:03:12.561-0400	INFO	main	org.flywaydb.core.internal.command.DbMigrate	Current version of schema `bell_logger`: << Empty Schema >>
2022-04-29T16:03:12.571-0400	INFO	main	org.flywaydb.core.internal.command.DbMigrate	Migrating schema `bell_logger` to version "1 - create schema"
2022-04-29T16:03:12.652-0400	INFO	main	org.flywaydb.core.internal.command.DbMigrate	Successfully applied 1 migration to schema `bell_logger`, now at version v1 (execution time 00:00.101s)
2022-04-29T16:03:12.663-0400	INFO	main	io.trino.dblistener.FlywayMigration	Performed 1 migrations
```

