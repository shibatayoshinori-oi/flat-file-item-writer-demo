package com.example.flatfileitemwriterdemo

import org.springframework.batch.core.ItemReadListener
import org.springframework.batch.core.ItemWriteListener
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder
import org.springframework.batch.item.support.ListItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource
import java.time.LocalDateTime

@Configuration
@EnableBatchProcessing
class BatchConfiguration(
    private val jobBuilderFactory: JobBuilderFactory,
    private val stepBuilderFactory: StepBuilderFactory,
    private val itemWriter: FlatFileItemWriter<String>
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
            .writer(itemWriter)
            .listener(itemReadListener())
            .listener(itemWriteListener())
            .build()
    }

    @StepScope
    @Bean
    fun itemReader(): ItemReader<Int> {
        return ListItemReader(IntRange(1, 3).toList())
    }

    @Bean
    fun itemReadListener(): ItemReadListener<Int> {
        return object : ItemReadListener<Int> {
            override fun beforeRead() {
                //NOOP
            }

            override fun afterRead(item: Int) {
                println("read $item")
            }

            override fun onReadError(ex: Exception) {
                //NOOP
            }
        }
    }

    @StepScope
    @Bean
    fun itemProcessor(): ItemProcessor<Int, String> {
        return ItemProcessor {
            println("process $it")
            it.toString()
        }
    }

    @StepScope
    @Bean
    fun itemWriter(
        @Value("#{stepExecution}") stepExecution: StepExecution
    ): FlatFileItemWriter<String> {
        return FlatFileItemWriterBuilder<String>()
            .name("sampleWriter")
            .resource(FileSystemResource("sample.csv.${LocalDateTime.now()}.csv"))
            .headerCallback {
                println("write header with stepExecution:$stepExecution")
                it.write("count:${stepExecution.readCount}")
            }
            .footerCallback {
                println("write footer with stepExecution:$stepExecution")
                it.write("count:${stepExecution.readCount}")
            }
            .lineAggregator { it }
            .build()
    }

    @Bean
    fun itemWriteListener(): ItemWriteListener<String> {
        return object : ItemWriteListener<String> {
            override fun beforeWrite(items: MutableList<out String>) {
                //NOOP
            }

            override fun afterWrite(items: MutableList<out String>) {
                println("wrote $items")
            }

            override fun onWriteError(ex: Exception, items: MutableList<out String>) {
                //NOOP
            }
        }
    }
}