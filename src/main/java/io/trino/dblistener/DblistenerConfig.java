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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

public class DblistenerConfig
{
    private String url;
    private String user;
    private String password;

    public String getUrl()
    {
        return url;
    }

    @Config("db-listener.url")
    public DblistenerConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }

    public String getUser()
    {
        return user;
    }

    @Config("db-listener.user")
    @ConfigDescription("Database user name")
    public DblistenerConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @ConfigSecuritySensitive
    @Config("db-listener.password")
    @ConfigDescription("Database password")
    public DblistenerConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
