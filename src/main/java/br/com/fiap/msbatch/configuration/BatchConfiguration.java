package br.com.fiap.msbatch.configuration;

import br.com.fiap.msbatch.model.Produto;
import br.com.fiap.msbatch.processor.ProdutoProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new ProdutoJobExecutionListener();
    }

    @Bean
    public Job processarProduto(JobRepository jobRepository, Step step, Step stepFim){
        return new JobBuilder("importProduto", jobRepository)
                .start(step)
                .next(stepFim)
                .listener(jobExecutionListener())
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository,
                     PlatformTransactionManager platformTransactionManager,
                     ItemReader<Produto> itemReader,
                     ItemWriter<Produto> itemWriter,
                     ItemProcessor<Produto,Produto> itemProcessor){
        return new StepBuilder("step", jobRepository)
                .<Produto,Produto>chunk(20, platformTransactionManager)
                .reader(itemReader)
                .writer(itemWriter)
                .processor(itemProcessor)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReader<Produto> itemReader(){
        BeanWrapperFieldSetMapper<Produto> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Produto.class);

        return new FlatFileItemReaderBuilder<Produto>()
                .name("personItemReader")
                .resource(new ClassPathResource("produto.csv"))
                .delimited()
                .names("id", "nome", "descricao", "quantidade", "preco")
                .fieldSetMapper(fieldSetMapper)
                .build();
    }

    @Bean
    public ItemWriter<Produto> itemWriter(DataSource dataSource){
        System.out.println("insere");

        return new JdbcBatchItemWriterBuilder<Produto>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .dataSource(dataSource)
                .sql("INSERT INTO tb_produtos "
                    + "(id_produto, nm_produto, ds_descricao, qt_estoque, pr_produto)"
                    + "values(:id, :nome, :descricao, :quantidade, :preco)"
                    + "ON DUPLICATE KEY UPDATE nm_produto=:nome,"
                    + " ds_descricao=:descricao, qt_estoque=:quantidade, pr_produto=:preco"
                )
                .build();

    }

    @Bean
    public ItemProcessor<Produto, Produto> itemProcessor(){
        return new ProdutoProcessor();
    }

}
