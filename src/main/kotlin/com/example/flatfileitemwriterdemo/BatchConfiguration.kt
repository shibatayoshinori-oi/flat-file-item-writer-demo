package com.example.flatfileitemwriterdemo

import org.springframework.batch.core.ItemReadListener
import org.springframework.batch.core.ItemWriteListener
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder
import org.springframework.batch.item.file.mapping.PassThroughLineMapper
import org.springframework.batch.item.support.ListItemReader
import org.springframework.batch.item.support.PassThroughItemProcessor
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource

@Configuration
@EnableBatchProcessing
class BatchConfiguration(
    private val jobBuilderFactory: JobBuilderFactory,
    private val stepBuilderFactory: StepBuilderFactory,
    private val itemWriter1: FlatFileItemWriter<String>,
    private val itemWriter2: FlatFileItemWriter<String>
) : DefaultBatchConfigurer() {

    @Bean
    fun job(): Job {
        return jobBuilderFactory.get("sampleJob")
            .start(step1())
            .next(step2())
            .build()
    }

    @Bean
    fun step1(): Step {
        return stepBuilderFactory.get("sampleStep1")
            .chunk<Int, String>(1_000)
            .reader(itemReader1())
            .processor(itemProcessor1())
            .writer(itemWriter1)
            .listener(itemReadListener1())
            .listener(itemWriteListener1())
            .build()
    }

    @StepScope
    @Bean
    fun itemReader1(): ItemReader<Int> {
        return ListItemReader(IntRange(1, 3).toList())
    }

    @Bean
    fun itemReadListener1(): ItemReadListener<Int> {
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
    fun itemProcessor1(): ItemProcessor<Int, String> {
        return ItemProcessor {
            println("process $it")
            it.toString()
        }
    }

    @StepScope
    @Bean
    fun itemWriter1(
        @Value("#{stepExecution}") stepExecution: StepExecution
    ): FlatFileItemWriter<String> {
        return FlatFileItemWriterBuilder<String>()
            .name("sampleWriter1")
            .resource(FileSystemResource("sample.csv.data-only"))
            .lineAggregator { it }
            .build()
    }

    @Bean
    fun itemWriteListener1(): ItemWriteListener<String> {
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

    @Bean
    fun step2(): Step {
        return stepBuilderFactory.get("sampleStep2")
            .chunk<String, String>(1_000)
            .reader(itemReader2())
            .processor(PassThroughItemProcessor())
            .writer(itemWriter2)
            .build()
    }

    @StepScope
    @Bean
    fun itemReader2(): FlatFileItemReader<String> {
        return FlatFileItemReaderBuilder<String>()
            .name("sampleReader2")
            .resource(FileSystemResource("sample.csv.data-only"))
            .lineMapper(PassThroughLineMapper())
            .build()
    }

    @StepScope
    @Bean
    fun itemWriter2(
        @Value("#{stepExecution.jobExecution}") jobExecution: JobExecution,
    ): FlatFileItemWriter<String> {
        return FlatFileItemWriterBuilder<String>()
            .name("sampleWriter2")
            .resource(FileSystemResource("sample.csv"))
            .headerCallback { writer ->
                val step1 = jobExecution.stepExecutions.find { it.stepName == "sampleStep1" }
                writer.write("this is a header. count:${step1!!.readCount}")
            }
            .footerCallback {
                it.write("this is a footer.")
            }
            .lineAggregator { it }
            .build()
    }
}