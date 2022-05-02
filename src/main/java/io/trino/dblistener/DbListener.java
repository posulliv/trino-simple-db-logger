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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.StageGcStatistics;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.util.List;

public class DbListener
        implements EventListener
{
    private static final Logger LOG = Logger.get(DbListener.class);
    private static final JsonCodec<List<StageGcStatistics>> STAGE_GC_STATS_CODEC = new JsonCodecFactory().listJsonCodec(StageGcStatistics.class);
    private final Jdbi jdbi;

    @Inject
    public DbListener(Jdbi jdbi)
    {
        this.jdbi = jdbi;
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        jdbi.useTransaction(handle -> {
            saveQuery(queryCompletedEvent, handle);
        });
    }

    private void saveQuery(QueryCompletedEvent queryCompletedEvent, Handle handle)
    {
        String sql = "" +
                "INSERT INTO queries (" +
                "  query_id," +
                "  catalog," +
                "  `schema`," +
                "  environment," +
                "  query_text," +
                "  query_plan," +
                "  created," +
                "  finished," +
                "  query_state," +
                "  error_info," +
                "  cpu_time," +
                "  failed_cpu_time," +
                "  wall_time," +
                "  queued_time," +
                "  scheduled_time," +
                "  failed_scheduled_time," +
                "  waiting_time," +
                "  analysis_time," +
                "  planning_time," +
                "  execution_time," +
                "  input_blocked_time," +
                "  failed_input_blocked_time," +
                "  output_blocked_time," +
                "  failed_output_blocked_time," +
                "  peak_user_memory_bytes," +
                "  peak_task_total_memory," +
                "  physical_input_bytes," +
                "  physical_input_rows," +
                "  processed_input_bytes," +
                "  processed_input_rows," +
                "  internal_network_bytes," +
                "  internal_network_rows," +
                "  total_bytes," +
                "  total_rows," +
                "  output_bytes," +
                "  output_rows," +
                "  written_bytes," +
                "  written_rows," +
                "  cumulative_memory," +
                "  failed_cumulative_memory," +
                "  completed_splits," +
                "  stage_gc_statistics" +
                ")" +
                "VALUES (" +
                "  :query_id," +
                "  :catalog," +
                "  :schema," +
                "  :environment," +
                "  :query_text," +
                "  :query_plan," +
                "  :created," +
                "  :finished," +
                "  :query_state," +
                "  :error_info," +
                "  :cpu_time," +
                "  :failed_cpu_time," +
                "  :wall_time," +
                "  :queued_time," +
                "  :scheduled_time," +
                "  :failed_scheduled_time," +
                "  :waiting_time," +
                "  :analysis_time," +
                "  :planning_time," +
                "  :execution_time," +
                "  :input_blocked_time," +
                "  :failed_input_blocked_time," +
                "  :output_blocked_time," +
                "  :failed_output_blocked_time," +
                "  :peak_user_memory_bytes," +
                "  :peak_task_total_memory," +
                "  :physical_input_bytes," +
                "  :physical_input_rows," +
                "  :processed_input_bytes," +
                "  :processed_input_rows," +
                "  :internal_network_bytes," +
                "  :internal_network_rows," +
                "  :total_bytes," +
                "  :total_rows," +
                "  :output_bytes," +
                "  :output_rows," +
                "  :written_bytes," +
                "  :written_rows," +
                "  :cumulative_memory," +
                "  :failed_cumulative_memory," +
                "  :completed_splits," +
                "  :stage_gc_statistics" +
                ")";

        QueryStatistics queryStatistics = queryCompletedEvent.getStatistics();
        String failureInfo = queryCompletedEvent.getFailureInfo().isPresent() ? queryCompletedEvent.getFailureInfo().get().getFailureMessage().get() : "";
        long scheduledTime = queryStatistics.getScheduledTime().isPresent() ? queryStatistics.getScheduledTime().get().toMillis() : 0;
        long failedScheduledTime = queryStatistics.getFailedScheduledTime().isPresent() ? queryStatistics.getFailedScheduledTime().get().toMillis() : 0;
        long waitingTime = queryStatistics.getResourceWaitingTime().isPresent() ? queryStatistics.getResourceWaitingTime().get().toMillis() : 0;
        long analysisTime = queryStatistics.getAnalysisTime().isPresent() ? queryStatistics.getAnalysisTime().get().toMillis() : 0;
        long planningTime = queryStatistics.getPlanningTime().isPresent() ? queryStatistics.getPlanningTime().get().toMillis() : 0;
        long executionTime = queryStatistics.getExecutionTime().isPresent() ? queryStatistics.getExecutionTime().get().toMillis() : 0;
        long inputBlockedTime = queryStatistics.getInputBlockedTime().isPresent() ? queryStatistics.getInputBlockedTime().get().toMillis() : 0;
        long failedInputBlockedTime = queryStatistics.getFailedInputBlockedTime().isPresent() ? queryStatistics.getFailedInputBlockedTime().get().toMillis() : 0;
        long outputBlockedTime = queryStatistics.getOutputBlockedTime().isPresent() ? queryStatistics.getOutputBlockedTime().get().toMillis() : 0;
        long failedOutputBlockedTime = queryStatistics.getFailedOutputBlockedTime().isPresent() ? queryStatistics.getFailedOutputBlockedTime().get().toMillis() : 0;

        handle.createUpdate(sql)
                .bind("query_id", queryCompletedEvent.getMetadata().getQueryId())
                .bind("catalog", queryCompletedEvent.getContext().getCatalog().orElse(""))
                .bind("schema", queryCompletedEvent.getContext().getSchema().orElse(""))
                .bind("environment", queryCompletedEvent.getContext().getEnvironment())
                .bind("query_text", queryCompletedEvent.getMetadata().getQuery())
                .bind("query_plan", queryCompletedEvent.getMetadata().getPlan().orElse(""))
                .bind("created", queryCompletedEvent.getCreateTime())
                .bind("finished", queryCompletedEvent.getEndTime())
                .bind("query_state", queryCompletedEvent.getMetadata().getQueryState())
                .bind("error_info", failureInfo)
                .bind("cpu_time", queryStatistics.getCpuTime().toMillis())
                .bind("failed_cpu_time", queryStatistics.getFailedCpuTime().toMillis())
                .bind("wall_time", queryStatistics.getWallTime().toMillis())
                .bind("queued_time", queryStatistics.getQueuedTime().toMillis())
                .bind("scheduled_time", scheduledTime)
                .bind("failed_scheduled_time", failedScheduledTime)
                .bind("waiting_time", waitingTime)
                .bind("analysis_time", analysisTime)
                .bind("planning_time", planningTime)
                .bind("execution_time", executionTime)
                .bind("input_blocked_time", inputBlockedTime)
                .bind("failed_input_blocked_time", failedInputBlockedTime)
                .bind("output_blocked_time", outputBlockedTime)
                .bind("failed_output_blocked_time", failedOutputBlockedTime)
                .bind("peak_user_memory_bytes", queryStatistics.getPeakUserMemoryBytes())
                .bind("peak_task_total_memory", queryStatistics.getPeakTaskTotalMemory())
                .bind("physical_input_bytes", queryStatistics.getPhysicalInputBytes())
                .bind("physical_input_rows", queryStatistics.getPhysicalInputRows())
                .bind("processed_input_bytes", queryStatistics.getProcessedInputBytes())
                .bind("processed_input_rows", queryStatistics.getProcessedInputRows())
                .bind("internal_network_bytes", queryStatistics.getInternalNetworkBytes())
                .bind("internal_network_rows", queryStatistics.getInternalNetworkRows())
                .bind("total_bytes", queryStatistics.getTotalBytes())
                .bind("total_rows", queryStatistics.getTotalRows())
                .bind("output_bytes", queryStatistics.getOutputBytes())
                .bind("output_rows", queryStatistics.getOutputRows())
                .bind("written_bytes", queryStatistics.getWrittenBytes())
                .bind("written_rows", queryStatistics.getWrittenRows())
                .bind("cumulative_memory", queryStatistics.getCumulativeMemory())
                .bind("failed_cumulative_memory", queryStatistics.getFailedCumulativeMemory())
                .bind("completed_splits", queryStatistics.getCompletedSplits())
                .bind("stage_gc_statistics", STAGE_GC_STATS_CODEC.toJson(queryStatistics.getStageGcStatistics()))
                .execute();
    }
}
