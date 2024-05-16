package br.com.fiap.msbatch.configuration;

import br.com.fiap.msbatch.model.Produto;
import br.com.fiap.msbatch.processor.ProdutoProcessor;
import br.com.fiap.msbatch.utils.Utilitarios;
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
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Random;

@PropertySource("classpath:application.yml")
@ConfigurationProperties(prefix = "data")
@Configuration
public class BatchConfiguration {

    private String path;
    private String arquivo;

    @Bean
    public JobExecutionListener jobExecutionListener() {
        Utilitarios util = new Utilitarios();
        this.arquivo = util.getNomeArquivo(this.getPath());
        return new ProdutoJobExecutionListener(this.arquivo);
    }

    @Bean
    public Job processarProduto(JobRepository jobRepository, Step step, Step stepFim){
        Random random = new Random();

        return new JobBuilder("importProduto" + random.toString(), jobRepository)
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
                .resource(  new FileSystemResource(this.arquivo))
                .delimited()
                .names("id", "nome", "descricao", "quantidade", "preco")
                .fieldSetMapper(fieldSetMapper)
                .build();
    }

    @Bean
    public ItemWriter<Produto> itemWriter(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Produto>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .dataSource(dataSource)
                .sql("INSERT INTO tb_produtos "
                    + "(id_produto, nm_produto, ds_descricao, qt_estoque, pr_produto, dt_update)"
                    + "values(:id, :nome, :descricao, :quantidade, :preco, :dataUpdate)"
                    + "ON DUPLICATE KEY UPDATE nm_produto=:nome,"
                    + " ds_descricao=:descricao, qt_estoque=:quantidade, pr_produto=:preco, dt_update=:dataUpdate"
                )
                .build();

    }

    @Bean
    public ItemProcessor<Produto, Produto> itemProcessor(){
        return new ProdutoProcessor();
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

}
