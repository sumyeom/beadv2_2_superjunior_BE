package store._0982.batch.batch.grouppurchase.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import store._0982.batch.domain.grouppurchase.GroupPurchaseRepository;
import store._0982.common.domain.grouppurchase.GroupPurchaseStatus;
import store._0982.common.log.BatchLogMessageFormat;
import store._0982.common.log.BatchLogMetadataFormat;

import java.time.Duration;
import java.time.OffsetDateTime;

@Slf4j
@Component
@StepScope
@RequiredArgsConstructor
public class UpdateStatusClosedGroupPurchaseStepListener implements StepExecutionListener {

    private static final String TARGET_OPEN_COUNT = "targetOpenCount";

    private final GroupPurchaseRepository groupPurchaseRepository;

    @Value("#{jobParameters['now']}")
    private String now;

    @Override
    public void beforeStep(StepExecution stepExecution){
        JobExecution jobExecution = stepExecution.getJobExecution();
        String jobName = jobExecution.getJobInstance().getJobName();
        String stepName = stepExecution.getStepName();
        Long jobExecutionId = stepExecution.getJobExecutionId();

        OffsetDateTime fixedNow = OffsetDateTime.parse(now);

        long targetOpenCount = groupPurchaseRepository.countByStatusAndEndDateLessThanEqual(
                GroupPurchaseStatus.OPEN,
                fixedNow
        );

        stepExecution.getExecutionContext().putLong(TARGET_OPEN_COUNT, targetOpenCount);

        log.info(
                BatchLogMessageFormat.stepStart(jobName, stepName),
                BatchLogMetadataFormat.stepStart(jobName,stepName,jobExecutionId)
        );
        log.info("updateStatusClosedGroupPurchaseStep started. now={}, targetOpenCount={}",
                fixedNow, targetOpenCount);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution){
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

        long targetOpenCount = stepExecution.getExecutionContext().containsKey(TARGET_OPEN_COUNT)
                ? stepExecution.getExecutionContext().getLong(TARGET_OPEN_COUNT)
                : -1L;

        if (targetOpenCount < 0) {
            String errorMessage = stepExecution.getFailureExceptions()
                    .stream()
                    .findFirst()
                    .map(Throwable::getMessage)
                    .orElse("beforeStep did not complete");

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
            return failStep(stepExecution);
        }

        long remainingOpenCount = groupPurchaseRepository.countByStatusAndEndDateLessThanEqual(
                GroupPurchaseStatus.OPEN,
                fixedNow
        );

        if(remainingOpenCount > 0){
            IllegalStateException exception =
                    new IllegalStateException(
                        "종료 시각이 지난 OPEN 공동구매가 남아 있습니다. remainingOpenCount = " + remainingOpenCount
                    );

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
            return failStep(stepExecution);
        }

        if(writeCount != targetOpenCount){
            IllegalStateException exception =
                    new IllegalStateException(
                            "종료 대상 건수와 실제 write 건수가 다릅니다. targetOpenCount=" + targetOpenCount + ", writeCount=" + writeCount
                    );

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

            return failStep(stepExecution);
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
        }else{
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
            log.info(
                    "updateStatusClosedGroupPurchaseStep verification passed. now={}, targetOpenCount={}, writeCount={}, remainingOpenCount=0",
                    fixedNow,
                    targetOpenCount,
                    writeCount
            );
        }

        return stepExecution.getExitStatus();
    }

    private ExitStatus failStep(StepExecution stepExecution) {
        stepExecution.setStatus(BatchStatus.FAILED);
        stepExecution.setExitStatus(ExitStatus.FAILED);
        return ExitStatus.FAILED;
    }
}
