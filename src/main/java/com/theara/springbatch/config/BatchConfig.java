package com.theara.springbatch.config;

import com.theara.springbatch.entity.Student;
import com.theara.springbatch.repository.StudentRepository;
import java.util.concurrent.Executor;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

  private final PlatformTransactionManager platformTransactionManager;
  private final JobRepository jobRepository;
  private final StudentRepository studentRepository;

  @Bean
  public FlatFileItemReader<Student> itemReader() {
    FlatFileItemReader<Student> itemReader = new FlatFileItemReader<>();
    itemReader.setResource(new FileSystemResource("src/main/resources/student.csv"));
    itemReader.setLinesToSkip(1);
    itemReader.setLineMapper(lineMapper());
    return itemReader;
  }

  @Bean
  public StudentProcessor processor() {
    return new StudentProcessor();
  }

  @Bean
  public RepositoryItemWriter<Student> writer() {
    RepositoryItemWriter<Student> writer = new RepositoryItemWriter<>();
    writer.setRepository(studentRepository);
    writer.setMethodName("save");
    return writer;
  }

  @Bean
  public Step importStep() {
    return new StepBuilder("csv-import", jobRepository)
        .<Student, Student>chunk(100, platformTransactionManager)
        .reader(itemReader())
        .processor(processor())
        .writer(writer())
        .taskExecutor(taskExecutor())
        .build();
  }

  @Bean
  public Job runJob() {
    return new JobBuilder("csv-job", jobRepository).start(importStep()).build();
  }

  private LineMapper<Student> lineMapper() {
    DefaultLineMapper<Student> lineMapper = new DefaultLineMapper<>();
    DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
    lineTokenizer.setDelimiter(",");
    lineTokenizer.setStrict(false);
    lineTokenizer.setNames("id", "firstname", "lastname", "age");

    BeanWrapperFieldSetMapper<Student> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
    fieldSetMapper.setTargetType(Student.class);
    lineMapper.setLineTokenizer(lineTokenizer);
    lineMapper.setFieldSetMapper(fieldSetMapper);

    return lineMapper;
  }

  @Bean("taskExecutor")
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setCorePoolSize(10);
    taskExecutor.setMaxPoolSize(20);
    taskExecutor.setQueueCapacity(1000);
    taskExecutor.setThreadNamePrefix("Import-csv-");
    taskExecutor.initialize();
    return taskExecutor;
  }
}
