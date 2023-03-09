package com.example.flatfileitemwriterdemo

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder
import org.springframework.batch.item.support.ListItemReader
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource

@Configuration
@EnableBatchProcessing
class BatchConfiguration(
    private val jobBuilderFactory: JobBuilderFactory,
    private val stepBuilderFactory: StepBuilderFactory
) : DefaultBatchConfigurer() {

    @Bean
    fun job(): Job {
        return jobBuilderFactory.get("sampleJob")
            .start(step())
            .build()
    }

    @Bean
    fun step(): Step {
        return stepBuilderFactory.get("sampleStep")
            .chunk<Int, String>(1_000)
            .reader(itemReader())
            .processor(itemProcessor())
            .writer(itemWriter())
            .build()
    }

    @StepScope
    @Bean
    fun itemReader(): ItemReader<Int> {
        return ListItemReader(IntRange(1, 5).toList())
    }

    @StepScope
    @Bean
    fun itemProcessor(): ItemProcessor<Int, String> {
        return ItemProcessor { it.toString() }
    }

    @StepScope
    @Bean
    fun itemWriter(): ItemWriter<String> {
        return FlatFileItemWriterBuilder<String>()
            .name("sampleWriter")
            .resource(FileSystemResource("sample.csv"))
            .headerCallback { it.write("header") }
            .footerCallback { it.write("footer") }
            .lineAggregator { it }
            .build()
    }
}