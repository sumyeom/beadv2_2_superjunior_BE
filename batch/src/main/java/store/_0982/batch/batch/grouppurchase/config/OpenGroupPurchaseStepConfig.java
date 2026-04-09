package store._0982.batch.batch.grouppurchase.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import store._0982.batch.batch.grouppurchase.dto.GroupPurchaseProjection;
import store._0982.batch.batch.grouppurchase.dto.GroupPurchaseResultProjection;
import store._0982.batch.batch.grouppurchase.listener.GroupPurchaseReaderListener;
import store._0982.batch.batch.grouppurchase.listener.GroupPurchaseWriterListener;
import store._0982.batch.batch.grouppurchase.listener.OpenGroupPurchaseStepListener;
import store._0982.batch.batch.grouppurchase.policy.GroupPurchasePolicy;
import store._0982.batch.batch.grouppurchase.processor.OpenGroupPurchaseProcessor;
import store._0982.batch.batch.grouppurchase.writer.OpenGroupPurchaseWriter;

/**
 * 공동구매 OPEN Step 설정
 */
@Configuration
@RequiredArgsConstructor
public class OpenGroupPurchaseStepConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    private final OpenGroupPurchaseProcessor groupPurchaseOpenProcessor;
    private final OpenGroupPurchaseWriter groupPurchaseOpenWriter;

    private final OpenGroupPurchaseStepListener openGroupPurchaseStepListener;
    private final GroupPurchaseReaderListener groupPurchaseReaderListener;
    private final GroupPurchaseWriterListener groupPurchaseWriterListener;

    @Bean
    public Step openGroupPurchaseStep(
            @Qualifier("openGroupPurchase") JpaCursorItemReader<GroupPurchaseProjection> openGroupPurchaseReader
    ) {
        return new StepBuilder("openGroupPurchaseStep", jobRepository)
                .<GroupPurchaseProjection, GroupPurchaseResultProjection>chunk(GroupPurchasePolicy.GroupPurchase.CHUNK_UNIT, transactionManager)
                .reader(openGroupPurchaseReader)
                .listener(groupPurchaseReaderListener)
                .processor(groupPurchaseOpenProcessor)
                .writer(groupPurchaseOpenWriter)
                .listener(groupPurchaseWriterListener)
                .listener(openGroupPurchaseStepListener)
                .build();
    }
}
