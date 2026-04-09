package store._0982.batch.batch.grouppurchase.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import store._0982.batch.batch.grouppurchase.dto.GroupPurchaseProjection;
import store._0982.common.log.BatchLogMessageFormat;
import store._0982.common.log.BatchLogMetadataFormat;

@Slf4j
@Component
@StepScope
public class GroupPurchaseReaderListener implements ItemReadListener<GroupPurchaseProjection> {

    private final StepExecution stepExecution;

    public GroupPurchaseReaderListener(@Value("#{stepExecution}") StepExecution stepExecution){
        this.stepExecution = stepExecution;
    }

    @Override
    public void onReadError(Exception ex){
        String jobName = stepExecution.getJobExecution().getJobInstance().getJobName();
        String stepName = stepExecution.getStepName();

        log.error(
                BatchLogMessageFormat.itemReaderFailed(jobName, stepName),
                BatchLogMetadataFormat.itemReaderFailed(
                        jobName,
                        stepName,
                        "groupPurchaseReader",
                        ex.getClass().getSimpleName(),
                        ex.getMessage()
                )
        );
    }
}
