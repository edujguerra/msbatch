package br.com.fiap.msbatch.configuration;

import org.springframework.batch.core.*;

public class ProdutoJobExecutionListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("entrou !!!!!!!!");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("Saiu !!!!!");
    }
}
