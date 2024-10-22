#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define N 37
#define KEY_FILE "incremental_key.bin"

//VARIAVEIS GLOBAIS
char dadosProdutos [20] = "dadosDoProduto.bin";
char dadosAcesso [20] = "dadosAcesso.bin";
char indiceProdutos [20] = "indiceProduto.bin";
char indiceAcesso [17] = "indiceAcesso.bin";

// Structs
typedef struct {
    int product_id;
    unsigned long long category_id;
    char category_code[N];
    char brand[N];
    float price;
} DadoProduto;

typedef struct {
    int incremental_key;
    int user_id;
    char user_session[N];
    char event_time[25];
    char event_type[20];
} DadoAcesso;

typedef struct {
    int product_id;
    long file_position;
} IndiceProduto;

typedef struct {
    int incremental_key;
    long file_position;
} IndiceAcesso;


// Protótipos funcoes
void parseRawLineToDadosProdutos(char *line, DadoProduto *registroProduto);
void parseRawLineToDadosAcesso(char *line, DadoAcesso *registroAcesso);
void criarArquivosDeDados();
bool productExists(int product_id, FILE *fDadosProdutos);
void insertProductInOrder(DadoProduto *newProduct);
void insertAcessoInOrder(DadoAcesso *newAcesso);
void padWithSpaces(char *str, size_t length);
char *strsep(char **stringp, const char *delim);
void criarIndiceProduto();
long buscarNoIndiceProduto(int product_id);
void printDadosProdutosComIndice(int product_id);

long buscarNoIndiceAcesso(int incremental_key) {
    FILE *fIndiceAcessos = fopen(indiceAcesso, "rb");
    if (fIndiceAcessos == NULL) {
        printf("Erro ao abrir o arquivo de índice de acessos.\n");
        return -1;
    }

    IndiceAcesso indice;
    int left = 0, right = 0;

    // Find out how many records are in the index file
    fseek(fIndiceAcessos, 0, SEEK_END);
    right = ftell(fIndiceAcessos) / sizeof(IndiceAcesso);
    rewind(fIndiceAcessos);

    // Binary search for the closest record in the index
    while (left <= right) {
        int mid = (left + right) / 2;
        fseek(fIndiceAcessos, mid * sizeof(IndiceAcesso), SEEK_SET);
        fread(&indice, sizeof(IndiceAcesso), 1, fIndiceAcessos);

        if (indice.incremental_key == incremental_key) {
            fclose(fIndiceAcessos);
            return indice.file_position;  // Exact match
        } else if (indice.incremental_key < incremental_key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    // If exact match not found, return the nearest lower position
    if (right < 0) {
        fclose(fIndiceAcessos);
        return 0;  // Start from the beginning if nothing is found
    }

    fseek(fIndiceAcessos, right * sizeof(IndiceAcesso), SEEK_SET);
    fread(&indice, sizeof(IndiceAcesso), 1, fIndiceAcessos);
    fclose(fIndiceAcessos);
    return indice.file_position;
}

void buscarAcesso(int incremental_key) {
    FILE *fDadosAcessos = fopen(dadosAcesso, "rb");
    if (fDadosAcessos == NULL) {
        printf("Erro ao abrir o arquivo de acessos.\n");
        return;
    }

    // Find the nearest index in the indiceAcesso file
    long pos = buscarNoIndiceAcesso(incremental_key);

    if (pos == -1) {
        printf("Acesso não encontrado.\n");
        fclose(fDadosAcessos);
        return;
    }

    fseek(fDadosAcessos, pos, SEEK_SET);

    DadoAcesso acesso;
    while (fread(&acesso, sizeof(DadoAcesso), 1, fDadosAcessos)) {
        if (acesso.incremental_key == incremental_key) {
            printf("Acesso encontrado: Incremental Key: %d, User ID: %d, Session: %s, Event Time: %s, Event Type: %s\n",
                   acesso.incremental_key, acesso.user_id, acesso.user_session, acesso.event_time, acesso.event_type);
            break;
        } else if (acesso.incremental_key > incremental_key) {
            printf("Acesso não encontrado.\n");
            break;
        }
    }

    fclose(fDadosAcessos);
}

void criarIndiceAcesso() {
    FILE *fDadosAcessos = fopen(dadosAcesso, "rb");
    FILE *fIndiceAcessos = fopen(indiceAcesso, "wb");

    if (fDadosAcessos == NULL || fIndiceAcessos == NULL) {
        printf("Erro ao abrir arquivos.\n");
        return;
    }

    DadoAcesso acesso;
    IndiceAcesso indice;
    int count = 0;

    while (fread(&acesso, sizeof(DadoAcesso), 1, fDadosAcessos)) {
        count++;

        // For every 100th record, store its position in the index file
        if (count % 100 == 0) {
            indice.incremental_key = acesso.incremental_key;
            indice.file_position = ftell(fDadosAcessos) - sizeof(DadoAcesso);  // Position of the record
            printf("Index created for key: %d at position: %ld\n", indice.incremental_key, indice.file_position);
            fwrite(&indice, sizeof(IndiceAcesso), 1, fIndiceAcessos);
        }
    }

    fclose(fDadosAcessos);
    fclose(fIndiceAcessos);
    printf("Arquivo de índice de acessos criado com sucesso.\n");
}

int getNextIncrementalKey() {
    FILE *fKey = fopen(KEY_FILE, "rb");
    int key = 0;

    if (fKey != NULL) {
        fread(&key, sizeof(int), 1, fKey);
        fclose(fKey);
    }

    key++;

    fKey = fopen(KEY_FILE, "wb");
    fwrite(&key, sizeof(int), 1, fKey);
    fclose(fKey);

    return key;
}

void criarIndiceProduto() {
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
    FILE *fIndiceProdutos = fopen(indiceProdutos, "wb");

    if (fDadosProdutos == NULL || fIndiceProdutos == NULL) {
        printf("Erro ao abrir arquivos.\n");
        return;
    }

    DadoProduto produto;
    IndiceProduto indice;
    int count = 0;

    while (fread(&produto, sizeof(DadoProduto), 1, fDadosProdutos)) {
        count++;

        if (count % 100 == 0) {
            indice.product_id = produto.product_id;
            indice.file_position = ftell(fDadosProdutos) - sizeof(DadoProduto);
            printf("Indices criados %d %ld", indice.product_id, indice.file_position);
            fwrite(&indice, sizeof(IndiceProduto), 1, fIndiceProdutos);
        }
    }

    fclose(fDadosProdutos);
    fclose(fIndiceProdutos);
    printf("indice criado com sucesso.\n");
}

long buscarNoIndice(int product_id) {
    FILE *fIndiceProdutos = fopen(indiceProdutos, "rb");
    if (fIndiceProdutos == NULL) {
        printf("Erro ao abrir o arquivo de índice.\n");
        return -1;
    }

    // Determine the number of records in the index file
    fseek(fIndiceProdutos, 0, SEEK_END);
    long file_size = ftell(fIndiceProdutos);
    int num_records = file_size / sizeof(IndiceProduto);
    rewind(fIndiceProdutos);

    IndiceProduto indice;
    int left = 0, right = num_records - 1;
    long start_position = 0;

    // Binary search in the index file
    while (left <= right) {
        int mid = (left + right) / 2;
        fseek(fIndiceProdutos, mid * sizeof(IndiceProduto), SEEK_SET);
        fread(&indice, sizeof(IndiceProduto), 1, fIndiceProdutos);

        if (indice.product_id == product_id) {
            // Exact match found
            start_position = indice.file_position;
            break;
        } else if (indice.product_id < product_id) {
            // Search in the right half
            start_position = indice.file_position;  // Save this position to search further
            left = mid + 1;
        } else {
            // Search in the left half
            right = mid - 1;
        }
    }

    fclose(fIndiceProdutos);
    return start_position;
}

void printDadosProdutosComIndice(int product_id) {
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
    if (fDadosProdutos == NULL) {
        printf("Erro ao abrir o arquivo de produtos.\n");
        return;
    }

    DadoProduto produto;
    long pos = buscarNoIndice(product_id);

    if (pos == -1) {
        printf("Produto não encontrado.\n");
        fclose(fDadosProdutos);
        return;
    }

    // Seek to the position in the product file and start searching
    fseek(fDadosProdutos, pos, SEEK_SET);

    // Linear search forward to find the exact product
    while (fread(&produto, sizeof(DadoProduto), 1, fDadosProdutos)) {
        if (produto.product_id >= product_id) {
            if (produto.product_id == product_id) {
                // Product found, print the details
                printf("Product ID: %d | Category ID: %llu | Brand: %s | Price: %.2f\n",
                       produto.product_id, produto.category_id, produto.brand, produto.price);
            } else {
                printf("Produto não encontrado.\n");
            }
            break;
        }
    }

    fclose(fDadosProdutos);
}

char *strsep(char **stringp, const char *delim) {
    char *start = *stringp;
    char *p;

    if (start == NULL) {
        return NULL;
    }

    p = strpbrk(start, delim);
    if (p) {
        *p = '\0';
        *stringp = p + 1;
    } else {
        *stringp = NULL;
    }

    return start;
}

void parseRawLineToDadosProdutos(char *line, DadoProduto *registroProduto) {
    char *token;  
    int i = 0;

    while ((token = strsep(&line, ",")) != NULL) {
        switch (i) {
            case 2:
                registroProduto->product_id = (*token) ? atoi(token) : 0;
                break;
            case 3:
                registroProduto->category_id = (*token) ? strtoull(token, NULL, 10) : 0;
                break;
            case 4:
                strncpy(registroProduto->category_code, token, N - 1);
                padWithSpaces(registroProduto->category_code, N - 1);
                break;
            case 5:
                strncpy(registroProduto->brand, token, N - 1);
                padWithSpaces(registroProduto->brand, N - 1);
                break;
            case 6:
                registroProduto->price = (*token) ? atof(token) : 0.0;
                break;
            default:
                break;
        }
        i++;
    }
}

void parseRawLineToDadosAcesso(char *line, DadoAcesso *registroAcesso) {
    char *token;
    int i = 0;

    while ((token = strsep(&line, ",")) != NULL) {
        switch (i) {
            case 0:
                strncpy(registroAcesso->event_time, token, 24);
                padWithSpaces(registroAcesso->event_time, 24);
                break;
            case 1:
                strncpy(registroAcesso->event_type, token, 19);
                padWithSpaces(registroAcesso->event_type, 19);
                break;
            case 7:
                registroAcesso->user_id = (*token) ? atoi(token) : 0;
                break;
            case 8:
                strncpy(registroAcesso->user_session, token, N - 1);
                padWithSpaces(registroAcesso->user_session, N - 1);
                break;
            default:
                break;
        }
        i++;
    }
}


void padWithSpaces(char *str, size_t length) {
    size_t currentLength = strlen(str);
    size_t i;
    for (i = currentLength; i < length; i++) {
        str[i] = ' ';
    }
    str[length] = '\0';
}

bool productExists(int product_id, FILE *fDadosProdutos) {
    DadoProduto tempProduct;
    rewind(fDadosProdutos);

    while (fread(&tempProduct, sizeof(DadoProduto), 1, fDadosProdutos)) {
        if (tempProduct.product_id == product_id) {
            return true;
        }
    }
    return false;
}

void insertProductInOrder(DadoProduto *newProduct) {
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
    FILE *tempFile = fopen("temp.bin", "wb");

    DadoProduto currentProduct;
    bool inserted = false;

    while (fread(&currentProduct, sizeof(DadoProduto), 1, fDadosProdutos)) {
        if (!inserted && newProduct->product_id < currentProduct.product_id) {
            fwrite(newProduct, sizeof(DadoProduto), 1, tempFile);
            inserted = true;
        }
        fwrite(&currentProduct, sizeof(DadoProduto), 1, tempFile);
    }

    if (!inserted) {
        fwrite(newProduct, sizeof(DadoProduto), 1, tempFile);
    }

    fclose(fDadosProdutos);
    fclose(tempFile);

    remove(dadosProdutos);
    rename("temp.bin", dadosProdutos);
}

void insertAcessoInOrder(DadoAcesso *newAcesso) {
    // Get the next available incremental key
    newAcesso->incremental_key = getNextIncrementalKey();

    FILE *fDadosAcessos = fopen(dadosAcesso, "ab");  // Use "ab" to append
    if (fDadosAcessos == NULL) {
        printf("Erro ao abrir o arquivo de acessos.\n");
        return;
    }

    // Write the new access record with the incremental key
    fwrite(newAcesso, sizeof(DadoAcesso), 1, fDadosAcessos);
    fclose(fDadosAcessos);
}

void criarArquivosDeDados() {
    char line[1024];
    int cont = 0;
    DadoAcesso registroAcesso;
    DadoProduto registroProduto;
    
    FILE *csv_file = fopen("2019-Nov.csv", "r");

    if (csv_file == NULL){
        printf("erro ao abrir o arquivo de nov");
        return;
    }

    fgets(line, sizeof(line), csv_file); //ignorar o cabecalho

    printf("Começou leitura e insercao dos dados\n");

    while (fgets(line, sizeof(line), csv_file) && cont <= 1000) {
        line[strcspn(line, "\n")] = 0;
        printf("Linha nmr: %d\n", cont++);

        char lineCopy1[1024], lineCopy2[1024];
        strncpy(lineCopy1, line, sizeof(lineCopy1));
        strncpy(lineCopy2, line, sizeof(lineCopy2));

        parseRawLineToDadosProdutos(lineCopy1, &registroProduto);
        parseRawLineToDadosAcesso(lineCopy2, &registroAcesso);

        insertAcessoInOrder(&registroAcesso);

        FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
        if (!productExists(registroProduto.product_id, fDadosProdutos)) {
            fclose(fDadosProdutos);
            insertProductInOrder(&registroProduto);
        } else {
            fclose(fDadosProdutos);
        }
    }

    printf("Terminou leitura e insercao dos dados\n");

    fclose(csv_file);
}

void printDadosProdutos() {
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
    if (fDadosProdutos == NULL) {
        printf("Erro ao abrir o arquivo de produtos.\n");
        return;
    }

    DadoProduto produto;
    printf("Listando produtos:\n");
    printf("--------------------------------------------------------------\n");
    printf("| Product ID | Category ID | Category Code   | Brand   | Price |\n");
    printf("--------------------------------------------------------------\n");

    int i =0;
    while (fread(&produto, sizeof(DadoProduto), 1, fDadosProdutos)) {
        printf("| %d | %d | %llu | %s | %s | %f |\n",
               i++,
               produto.product_id,
               produto.category_id,
               produto.category_code,
               produto.brand,
               produto.price);
    }

    printf("--------------------------------------------------------------\n");

    fclose(fDadosProdutos);
}

void printDadosAcessos() {
    FILE *fDadosAcessos = fopen(dadosAcesso, "rb");
    if (fDadosAcessos == NULL) {
        printf("Erro ao abrir o arquivo de acessos.\n");
        return;
    }

    DadoAcesso acesso;
    printf("Listando acessos:\n");
    printf("--------------------------------------------------------------------------------------\n");
    printf("| User ID  | User Session          | Event Time             | Event Type             |\n");
    printf("--------------------------------------------------------------------------------------\n");

    while (fread(&acesso, sizeof(DadoAcesso), 1, fDadosAcessos)) {
        printf("| %d | %d | %s | %s | %s |\n",
               acesso.incremental_key,
               acesso.user_id,
               acesso.user_session,
               acesso.event_time,
               acesso.event_type);
    }

    printf("--------------------------------------------------------------------------------------\n");

    fclose(fDadosAcessos);
}

int main() {
    bool x = true;

    while (x) {
        printf("\nEscolha as opcoes:\n");
        printf("1 - Criar os arquivos de dados\n");
        printf("2 - Mostrar todos os dados dos produtos\n");
        printf("3 - Mostrar todos os dados dos acessos\n");
        printf("4 - Criar Arquivo de índice produto\n");
        printf("5 - Procurar produto por ID\n");
        printf("6 - Criar indice acesso\n");
        printf("7 - Procurar indice acesso\n");
        printf("8 - Sair\n");

        int resposta;
        scanf("%d", &resposta);

        switch (resposta) {
            case 1:
                criarArquivosDeDados();
                printf("Arquivos de dados criados com sucesso.\n");
                break;

            case 2:
                printDadosProdutos();
                break;
            case 3:
                printDadosAcessos();
                break;
            case 4:
                criarIndiceProduto();
                printf("Arquivo de indices criado com sucesso.\n");
                break;

            case 5: {
                int product_id;
                printf("Digite o ID do produto a ser procurado: ");
                scanf("%d", &product_id);
                printDadosProdutosComIndice(product_id);
                break;
            }

            case 6:
                criarIndiceAcesso();
                printf("indices criados com sucesso!");
                break;

            case 7:{
                int incremental_key;

                printf("Digite o id a ser procurada: ");
                scanf("%d", &incremental_key);

                buscarAcesso(incremental_key);
                break;
            }

            case 8:
                x = false;
                printf("Encerrando o programa.\n");
                break;
            default:
                printf("Opção inválida. Tente novamente.\n");
                break;
        }
    }

    return 0;
}