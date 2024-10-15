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

typedef struct 
{
    int user_id;
    char user_session [N];
    char event_time[25];
    char event_type[20];
} DadoAcesso;


void parseRawLineToDadosProdutos(char *line, DadoProduto *registroProduto){
    char *token;

    memset(registroProduto->category_code, '\0', N);
    memset(registroProduto->brand,'\0',N);
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

void parseRawLineToDadosAcesso(char *line, DadoAcesso *registroAcesso){
    char *token;

    memset(registroAcesso->event_time, '\0', 25);
    memset(registroAcesso->event_type, '\0', 20);
    memset(registroAcesso->user_session, '\0', N);

    // Skip event_time
    token = strtok(NULL, ",");
    strncpy(registroAcesso->event_time, token ? token : "",  25);
    
    // Skip event_type
    token = strtok(NULL, ",");
    strncpy(registroAcesso->event_type, token ? token : "", 20);

    // Extract product_id
    strtok(NULL, ",");

    // Extract category_id
    strtok(NULL, ",");

    // Extract category_code
    strtok(NULL, ",");

    // Extract brand
    strtok(NULL, ",");

    // Extract price
    strtok(NULL, ",");

    // Extract user_id
    registroAcesso->user_id = atoi(strtok(NULL, ""));

    // Extract user_session
    token = strtok(NULL, ",");
    strncpy(registroAcesso->user_session, token ? token : "", N);
}

void criarArquivosDeDados(){
    char line[1024];
    int cont = 0;

    FILE *csv_file = fopen("2019-Nov.csv", "r");
    FILE *fDadosProdutos = fopen(dadosProdutos, "wb");
    FILE *fDadosAcessos = fopen(dadosAcesso, "wb");

    DadoAcesso registroAcesso;
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
        printf("%d", cont++);
        parseRawLineToDadosProdutos(line, &registroProduto);
        parseRawLineToDadosAcesso(line, &registroAcesso);

        fwrite(&registroAcesso, sizeof(registroAcesso),1,fDadosAcessos);
        fwrite(&registroProduto, sizeof(registroProduto),1,fDadosProdutos);
    }

    printf("Terminou leitura e inserção dos dados");
    fclose(csv_file);
    fclose(fDadosProdutos);
    fclose(fDadosAcessos);
}

int main(){
    criarArquivosDeDados();
}