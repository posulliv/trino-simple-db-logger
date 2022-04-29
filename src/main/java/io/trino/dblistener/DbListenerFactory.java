/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.dblistener;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigurationUtils.replaceEnvironmentVariables;

public class DbListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "db-event-listener";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        config = replaceEnvironmentVariables(config);
        FlywayMigration.migrate(new ConfigurationFactory(config).build(DblistenerConfig.class));
        Bootstrap app = new Bootstrap(
                new DbModule());
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(DbListener.class);
    }
}
