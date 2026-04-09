package store._0982.batch.batch.grouppurchase.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import store._0982.batch.batch.grouppurchase.dto.GroupPurchaseResultProjection;
import store._0982.common.log.BatchLogMessageFormat;
import store._0982.common.log.BatchLogMetadataFormat;

@Slf4j
@Component
@StepScope
public class GroupPurchaseWriterListener implements ItemWriteListener<GroupPurchaseResultProjection> {
    private final StepExecution stepExecution;

    public GroupPurchaseWriterListener(@Value("#{stepExecution}") StepExecution stepExecution){
        this.stepExecution = stepExecution;
    }

    @Override
    public void onWriteError(Exception ex, Chunk<? extends GroupPurchaseResultProjection> items){
        String jobName = stepExecution.getJobExecution().getJobInstance().getJobName();
        String stepName = stepExecution.getStepName();

        log.error(
                BatchLogMessageFormat.itemWriterFailed(jobName, stepName),
                BatchLogMetadataFormat.itemWriterFailed(
                        jobName,
                        stepName,
                        "groupPurchaseWriter",
                        ex.getClass().getSimpleName(),
                        ex.getMessage()
                )
        );
    }
}
