#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N 100

//VARIAVEIS GLOBAIS
char dadosProdutos [20] = "dadosDoProduto.bin";
char dadosAcesso [20] = "dadosAcesso.bin";
char indiceProdutos [20] = "indiceProduto.bin";
char indiceAcesso [16] = "indiceAcesso.bin";

typedef struct
{
    int product_id;
    long category_id;
    char category_code[N];
    char brand[N];
    float price;
} DadoProduto;  

void parseRawLineToDadosProdutos(char *line, DadoProduto *registroProduto){
    char *token;

    // Skip event_time
    token = strtok(line, ",");
    
    // Skip event_type
    token = strtok(NULL, ",");

    // Extract product_id
    registroProduto->product_id = atoi(strtok(NULL, ","));

    // Extract category_id
    registroProduto->category_id = atol(strtok(NULL, ","));

    // Extract category_code
    token = strtok(NULL, ",");
    strncpy(registroProduto->category_code, token ? token : "", N);

    // Extract brand
    token = strtok(NULL, ",");
    strncpy(registroProduto->brand, token ? token : "", N);

    // Extract price
    registroProduto->price = atof(strtok(NULL, ","));
}

void criarArquivosDeDados(){
    FILE *csv_file = fopen("2019-Nov.csv", "r");
    FILE *fDadosProdutos = fopen(dadosAcesso, "wb");
    char line[1024];
    DadoProduto registroProduto;

    if (csv_file == NULL){
        printf("erro ao abrir o arquivo");
        return;
    }

    if (fDadosProdutos == NULL){
        printf("Erro ao abrir o arquivo de produtos");
        return;
    }

    fgets(line, sizeof(line), csv_file);//ignorar o cabecalho

    printf("Começou leitura e inserção dos dados");
    
    while(fgets(line, sizeof(line), csv_file)){
        line[strcspn(line, "\n")] = 0;
        parseRawLineToDadosProdutos(line, &registroProduto);
        fwrite(&registroProduto, sizeof(registroProduto),1,fDadosProdutos);
    }

    printf("Terminou leitura e inserção dos dados");
    fclose(csv_file);
    fclose(fDadosProdutos);
}

int main(){
    criarArquivosDeDados();
}