package br.com.fiap.msbatch.configuration;

import org.springframework.batch.core.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;

public class ProdutoJobExecutionListener implements JobExecutionListener {

    private String arquivo;

    public ProdutoJobExecutionListener(String arquivo){
        this.arquivo = arquivo;
        System.out.println(arquivo);
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        File arquivoOriginal = new File(arquivo);
        String novoNome = arquivo.substring(0, arquivo.length() - 4) + ".ant";
        File novoArquivo = new File(novoNome);
        arquivoOriginal.renameTo(novoArquivo);
        arquivoOriginal.delete();
        System.out.println("Deletar : " + arquivo);
    }
}
