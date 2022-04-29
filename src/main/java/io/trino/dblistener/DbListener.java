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

import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;

public class DbListener
        implements EventListener
{
    private static final Logger LOG = Logger.get(DbListener.class);

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        LOG.info("query ID is: %s", queryCompletedEvent.getMetadata().getQueryId());
    }
}
