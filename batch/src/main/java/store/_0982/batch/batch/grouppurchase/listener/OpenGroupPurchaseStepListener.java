package store._0982.batch.batch.grouppurchase.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataUnit;
import store._0982.batch.domain.grouppurchase.GroupPurchaseRepository;
import store._0982.common.domain.grouppurchase.GroupPurchaseStatus;
import store._0982.common.log.BatchLogMessageFormat;
import store._0982.common.log.BatchLogMetadataFormat;

import java.time.Duration;
import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
@StepScope
@Slf4j
public class OpenGroupPurchaseStepListener implements StepExecutionListener {

    private final GroupPurchaseRepository groupPurchaseRepository;

    @Value("#{jobParameters['now']}")
    private String now;

    @Override
    public void beforeStep(StepExecution stepExecution){
        JobExecution jobExecution = stepExecution.getJobExecution();
        String jobName = jobExecution.getJobInstance().getJobName();
        String stepName = stepExecution.getStepName();
        Long jobExecutionId = stepExecution.getJobExecutionId();

        log.info(
                BatchLogMessageFormat.stepStart(jobName, stepName),
                BatchLogMetadataFormat.stepStart(
                        jobName,
                        stepName,
                        jobExecutionId
                )
        );
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        JobExecution jobExecution = stepExecution.getJobExecution();
        String jobName = jobExecution.getJobInstance().getJobName();
        String stepName = stepExecution.getStepName();

        long duration = -1L;
        if(stepExecution.getStartTime() != null && stepExecution.getEndTime() != null){
            duration = Duration.between(
                    stepExecution.getStartTime(),
                    stepExecution.getEndTime()
            ).toMillis();
        }

        long readCount = stepExecution.getReadCount();
        long writeCount = stepExecution.getWriteCount();
        long skipCount = stepExecution.getSkipCount();

        OffsetDateTime fixedNow = OffsetDateTime.parse(now);

        long remaining = groupPurchaseRepository.countByStatusAndStartDateLessThanEqual(
                GroupPurchaseStatus.SCHEDULED,
                fixedNow
        );

        if (remaining > 0) {
            IllegalStateException exception = new IllegalStateException("남은 SCHEDULED 공동 구매가 존재합니다. count = " + remaining);
            stepExecution.addFailureException(exception);

            log.error(
                    BatchLogMessageFormat.stepFailed(jobName, stepName),
                    BatchLogMetadataFormat.stepFailed(
                            jobName,
                            stepName,
                            readCount,
                            writeCount,
                            skipCount,
                            duration,
                            exception.getMessage()
                    )
            );

            return ExitStatus.FAILED;
        }

        if(stepExecution.getStatus().isUnsuccessful()){
            String errorMessage = stepExecution.getFailureExceptions()
                    .stream()
                    .findFirst()
                    .map(Throwable::getMessage)
                    .orElse("UNKNOWN");

            log.error(
                    BatchLogMessageFormat.stepFailed(jobName, stepName),
                    BatchLogMetadataFormat.stepFailed(
                            jobName,
                            stepName,
                            readCount,
                            writeCount,
                            skipCount,
                            duration,
                            errorMessage
                    )
            );
        }else {
            log.info(
                    BatchLogMessageFormat.stepSuccess(jobName, stepName),
                    BatchLogMetadataFormat.stepSuccess(
                            jobName,
                            stepName,
                            readCount,
                            writeCount,
                            skipCount,
                            duration
                    )
            );
            log.info("openGroupPurchaseStep verification passed. now={}, remainingCount=0", fixedNow);
        }
        return stepExecution.getExitStatus();
    }
}
