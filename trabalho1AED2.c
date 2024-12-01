#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define N 37

#define MAX 99
#define MIN 49
#define KEY_FILE "incremental_key.bin"
#define HASH_SIZE 10000


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
    bool deleted;
} DadoProduto;

typedef struct {
    int incremental_key;
    int user_id;
    char user_session[N];
    char event_time[25];
    char event_type[20];
    bool deleted;
} DadoAcesso;

typedef struct {
    int product_id;
    long file_position;
} IndiceProduto;

typedef struct {
    int incremental_key;
    long file_position;
} IndiceAcesso;

struct BTreeNode {
  int product_id[MAX + 1], count;     // Armazena as chaves
  long file_position[MAX + 1];       // Armazena os endereços do arquivo
  struct BTreeNode *link[MAX + 1];   // Ponteiros para os filhos
};
struct BTreeNode *root = NULL;

struct BTreeNode *createNode(int product_id, struct BTreeNode *child, long file_position) {
  struct BTreeNode *newNode;
  newNode = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
  newNode->product_id[1] = product_id;
  newNode->file_position[1] = file_position; 
  newNode->count = 1;
  newNode->link[0] = root;
  newNode->link[1] = child;
  return newNode;
}

typedef struct HashNode {
    int incremental_key;
    long file_position;
    struct HashNode* next;
} HashNode;
HashNode* hashTable[HASH_SIZE];

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

int hashFunction(int key) {
    return key % HASH_SIZE;
}

void initHashTable() {
    int i;
    for (i = 0; i < HASH_SIZE; i++) {
        hashTable[i] = NULL;
    }
}

void insertIntoHashTable(int incremental_key, long file_position) {
    int hashIndex = hashFunction(incremental_key);
    HashNode* newNode = (HashNode*)malloc(sizeof(HashNode));
    newNode->incremental_key = incremental_key;
    newNode->file_position = file_position;
    newNode->next = hashTable[hashIndex];
    hashTable[hashIndex] = newNode;
}

long searchInHashTable(int incremental_key) {
    int hashIndex = hashFunction(incremental_key);
    HashNode* current = hashTable[hashIndex];
    while (current) {
        if (current->incremental_key == incremental_key) {
            return current->file_position;
        }
        current = current->next;
    }
    return -1; // Não encontrado
}

void freeHashTable() {
    int i;
    for (i = 0; i < HASH_SIZE; i++) {
        HashNode* current = hashTable[i];
        while (current) {
            HashNode* temp = current;
            current = current->next;
            free(temp);
        }
        hashTable[i] = NULL;
    }
}

void criarHashTableAcessos() {
    printf("Criando tabela hash");
    FILE *fDadosAcessos = fopen(dadosAcesso, "rb");
    if (fDadosAcessos == NULL) {
        printf("Erro ao abrir o arquivo de acessos.\n");
        return;
    }

    initHashTable();
    DadoAcesso acesso;
    while (fread(&acesso, sizeof(DadoAcesso), 1, fDadosAcessos)) {
        if (!acesso.deleted) {
            long position = ftell(fDadosAcessos) - sizeof(DadoAcesso);
            insertIntoHashTable(acesso.incremental_key, position);
        }
    }

    fclose(fDadosAcessos);
    printf("Tabela hash criada com sucesso.\n");
}

// Inserir entrada em um nó
void insertNode(int product_id, long file_position, int pos, struct BTreeNode *node, struct BTreeNode *child) {
  int j = node->count;
  while (j > pos) {
    node->product_id[j + 1] = node->product_id[j];
    node->file_position[j + 1] = node->file_position[j];
    node->link[j + 1] = node->link[j];
    j--;
  }
  node->product_id[j + 1] = product_id;
  node->file_position[j + 1] = file_position;
  node->link[j + 1] = child;
  node->count++;
}

// Dividir nó quando estiver cheio
void splitNode(int product_id, long file_position, int *pproduct_id, long *pfile_position, int pos,
               struct BTreeNode *node, struct BTreeNode *child, struct BTreeNode **newNode) {
  int median, j;

  if (pos > MIN)
    median = MIN + 1;
  else
    median = MIN;

  *newNode = (struct BTreeNode *)malloc(sizeof(struct BTreeNode));
  j = median + 1;
  while (j <= MAX) {
    (*newNode)->product_id[j - median] = node->product_id[j];
    (*newNode)->file_position[j - median] = node->file_position[j];
    (*newNode)->link[j - median] = node->link[j];
    j++;
  }
  node->count = median;
  (*newNode)->count = MAX - median;

  if (pos <= MIN) {
    insertNode(product_id, file_position, pos, node, child);
  } else {
    insertNode(product_id, file_position, pos - median, *newNode, child);
  }
  *pproduct_id = node->product_id[node->count];
  *pfile_position = node->file_position[node->count];
  (*newNode)->link[0] = node->link[node->count];
  node->count--;
}

// Inserir valor recursivamente na árvore
int setValue(int product_id, long file_position, int *pproduct_id, long *pfile_position,
             struct BTreeNode *node, struct BTreeNode **child) {
  int pos;
  if (!node) {
    *pproduct_id = product_id;
    *pfile_position = file_position;
    *child = NULL;
    return 1;
  }

  if (product_id < node->product_id[1]) {
    pos = 0;
  } else {
    for (pos = node->count;
         (product_id < node->product_id[pos] && pos > 1); pos--)
      ;
    if (product_id == node->product_id[pos]) {
      printf("Chave duplicada não permitida\n");
      return 0;
    }
  }

  if (setValue(product_id, file_position, pproduct_id, pfile_position, node->link[pos], child)) {
    if (node->count < MAX) {
      insertNode(*pproduct_id, *pfile_position, pos, node, *child);
    } else {
      struct BTreeNode *newChild;
      splitNode(*pproduct_id, *pfile_position, pproduct_id, pfile_position, pos, node, *child, &newChild);
      *child = newChild;
      return 1;
    }
  }
  return 0;
}

// Inserir na árvore
void insertInBTree(int product_id, long file_position) {
  int flag, i;
  long file_pos;
  struct BTreeNode *child;

  flag = setValue(product_id, file_position, &i, &file_pos, root, &child);
  if (flag)
    root = createNode(i, child, file_pos);
}

// Buscar na árvore
void search(int product_id, long *file_position, struct BTreeNode *myNode) {
  if (!myNode) {
    *file_position = -1;
    return;
  }

  int pos;
  if (product_id < myNode->product_id[1]) {
    pos = 0;
  } else {
    for (pos = myNode->count;
         (product_id < myNode->product_id[pos] && pos > 1); pos--)
      ;
    if (product_id == myNode->product_id[pos]) {
      *file_position = myNode->file_position[pos];
      return;
    }
  }
  search(product_id, file_position, myNode->link[pos]);
}

// Percorrer a árvore
void traversal(struct BTreeNode *myNode) {
  int i;
  if (myNode) {
    for (i = 0; i < myNode->count; i++) {
      traversal(myNode->link[i]);
      printf("Chave: %d, Endereço: %ld\n", myNode->product_id[i + 1], myNode->file_position[i + 1]);
    }
    traversal(myNode->link[myNode->count]);
  }
}

void criarBtree(){
    printf("\nCriando B-TREE\n");
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
    FILE *fExtensionArea = fopen("extension.bin", "rb");

    if (fDadosProdutos == NULL) {
        printf("Erro ao abrir o arquivo de produtos.\n");
        return;
    }

    DadoProduto produto;

    while (fread(&produto, sizeof(DadoProduto), 1, fDadosProdutos)) {
        if (!produto.deleted) {
            insertInBTree(produto.product_id, ftell(fDadosProdutos) - sizeof(DadoProduto));
        }
    }

    if (fExtensionArea != NULL) {
        while (fread(&produto, sizeof(DadoProduto), 1, fExtensionArea)) {
            printf("| %d | %llu | %s | %s | %.2f |\n",
                   produto.product_id,
                   produto.category_id,
                   produto.category_code,
                   produto.brand,
                   produto.price);
        }
        fclose(fExtensionArea);
    }

    printf("--------------------------------------------------------------\n");

    fclose(fDadosProdutos);
    printf("\nBtree criada com sucesso!\n");
}

void insertAcesso(DadoAcesso *newAcesso) {
    FILE *fDadosAcessos = fopen(dadosAcesso, "ab+");
    if (fDadosAcessos == NULL) {
        printf("Erro ao abrir o arquivo de acessos.\n");
        return;
    }

    newAcesso->incremental_key = getNextIncrementalKey();
    newAcesso->deleted = false;
    fwrite(newAcesso, sizeof(DadoAcesso), 1, fDadosAcessos);

    fclose(fDadosAcessos);
    printf("Novo acesso inserido com sucesso. Incremental Key: %d\n", newAcesso->incremental_key);
}
void deleteAcesso(int incremental_key) {
    FILE *fDadosAcessos = fopen(dadosAcesso, "rb+");
    if (fDadosAcessos == NULL) {
        printf("Erro ao abrir o arquivo de acessos.\n");
        return;
    }

    DadoAcesso acesso;
    bool found = false;

    while (fread(&acesso, sizeof(DadoAcesso), 1, fDadosAcessos)) {
        if (acesso.incremental_key == incremental_key && !acesso.deleted) {
            acesso.deleted = true;
            fseek(fDadosAcessos, -sizeof(DadoAcesso), SEEK_CUR);
            fwrite(&acesso, sizeof(DadoAcesso), 1, fDadosAcessos);
            found = true;
            printf("Acesso ID %d removido logicamente.\n", incremental_key);
            break;
        }
    }

    if (!found) {
        printf("Acesso ID %d não encontrado.\n", incremental_key);
    }

    fclose(fDadosAcessos);
}

void reorganizeAcessoFile() {
    FILE *fDadosAcessos = fopen(dadosAcesso, "rb");
    FILE *tempFile = fopen("temp_acesso.bin", "wb");

    if (!fDadosAcessos || !tempFile) {
        printf("Erro ao abrir os arquivos durante a reorganização.\n");
        return;
    }

    DadoAcesso acesso;

    while (fread(&acesso, sizeof(DadoAcesso), 1, fDadosAcessos)) {
        if (!acesso.deleted) {
            fwrite(&acesso, sizeof(DadoAcesso), 1, tempFile);
        }
    }

    fclose(fDadosAcessos);
    fclose(tempFile);

    remove(dadosAcesso);
    rename("temp_acesso.bin", dadosAcesso);

    printf("Reorganização completa. Registros excluídos removidos.\n");
}

void insertProductWithExtension(DadoProduto *newProduct) {
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb+");
   // FILE *fExtensionArea = fopen("extension.bin", "ab+");

    if (!fDadosProdutos /*|| !fExtensionArea*/) {
        printf("Erro ao abrir os arquivos de produtos ou extensao.\n");
        return;
    }

    DadoProduto currentProduct;
    bool inserted = false;

    FILE *tempFile = fopen("temp.bin", "wb");
    if (!tempFile) {
        printf("Erro ao abrir o arquivo temporario.\n");
        fclose(fDadosProdutos);
        return;
    }

    fseek(fDadosProdutos, 0, SEEK_SET);
    while (fread(&currentProduct, sizeof(DadoProduto), 1, fDadosProdutos)) {
        if (currentProduct.deleted) {
            continue;
        }

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
    //fclose(fExtensionArea);
    fclose(tempFile);

    remove(dadosProdutos);
    rename("temp.bin", dadosProdutos);

    printf("Produto ID %d inserido no arquivo de produtos ordenado.\n", newProduct->product_id);
}


void removeProduct(int product_id) {
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb+");

    if (!fDadosProdutos) {
        printf("Erro ao abrir o arquivo de produtos.\n");
        return;
    }

    DadoProduto currentProduct;
    bool found = false;

    while (fread(&currentProduct, sizeof(DadoProduto), 1, fDadosProdutos)) {
        if (currentProduct.product_id == product_id && !currentProduct.deleted) {
            currentProduct.deleted = true;

            fseek(fDadosProdutos, -sizeof(DadoProduto), SEEK_CUR);
            fwrite(&currentProduct, sizeof(DadoProduto), 1, fDadosProdutos);
            found = true;
            printf("Produto ID %d removido logicamente.\n", product_id);
            break;
        }
    }

    if (!found) {
        printf("Produto ID %d nao encontrado.\n", product_id);
    }

    fclose(fDadosProdutos);
}

void reorganizeFile() {
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
    FILE *fExtensionArea = fopen("extension.bin", "rb");
    FILE *tempFile = fopen("temp.bin", "wb");

    if (!fDadosProdutos || !tempFile) {
        printf("Erro ao abrir os arquivos durante a reorganizacao.\n");
        return;
    }

    DadoProduto productFromMain, productFromExtension;
    bool mainEmpty = !fread(&productFromMain, sizeof(DadoProduto), 1, fDadosProdutos);
    bool extensionEmpty = true;

    if (fExtensionArea) {
        extensionEmpty = !fread(&productFromExtension, sizeof(DadoProduto), 1, fExtensionArea);
    }

    while (!mainEmpty || !extensionEmpty) {
        if (!mainEmpty && productFromMain.deleted) {
            mainEmpty = !fread(&productFromMain, sizeof(DadoProduto), 1, fDadosProdutos);
            continue;
        }

        if (extensionEmpty || (!mainEmpty && productFromMain.product_id < productFromExtension.product_id)) {
            fwrite(&productFromMain, sizeof(DadoProduto), 1, tempFile);
            mainEmpty = !fread(&productFromMain, sizeof(DadoProduto), 1, fDadosProdutos);
        } else {
            fwrite(&productFromExtension, sizeof(DadoProduto), 1, tempFile);
            extensionEmpty = !fread(&productFromExtension, sizeof(DadoProduto), 1, fExtensionArea);
        }
    }

    fclose(fDadosProdutos);
    if (fExtensionArea) fclose(fExtensionArea);
    fclose(tempFile);

    remove(dadosProdutos);
    rename("temp.bin", dadosProdutos);

    if (fExtensionArea) {
        remove("extension.bin");
    }

    printf("Reorganizacao completa com produtos ordenados por ID.\n");
}


long buscarNoIndiceAcesso(int incremental_key) {
    FILE *fIndiceAcessos = fopen(indiceAcesso, "rb");
    if (fIndiceAcessos == NULL) {
        printf("Erro ao abrir o arquivo de indice de acessos.\n");
        return -1;
    }

    IndiceAcesso indice;
    int left = 0, right = 0;

    fseek(fIndiceAcessos, 0, SEEK_END);
    right = ftell(fIndiceAcessos) / sizeof(IndiceAcesso);
    rewind(fIndiceAcessos);

    while (left <= right) {
        int mid = (left + right) / 2;
        fseek(fIndiceAcessos, mid * sizeof(IndiceAcesso), SEEK_SET);
        fread(&indice, sizeof(IndiceAcesso), 1, fIndiceAcessos);

        if (indice.incremental_key == incremental_key) {
            fclose(fIndiceAcessos);
            return indice.file_position;
        } else if (indice.incremental_key < incremental_key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    if (right < 0) {
        fclose(fIndiceAcessos);
        return 0;
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

    long pos = buscarNoIndiceAcesso(incremental_key);

    if (pos == -1) {
        printf("Acesso nao encontrado.\n");
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
            printf("Acesso nao encontrado.\n");
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

        if (count % 100 == 0) {
            indice.incremental_key = acesso.incremental_key;
            indice.file_position = ftell(fDadosAcessos) - sizeof(DadoAcesso);
            printf("Index criado para a chave: %d na posicao: %ld\n", indice.incremental_key, indice.file_position);
            fwrite(&indice, sizeof(IndiceAcesso), 1, fIndiceAcessos);
        }
    }

    fclose(fDadosAcessos);
    fclose(fIndiceAcessos);
    printf("Arquivo de indice de acessos criado com sucesso.\n");
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

long buscarNoIndiceProduto(int product_id) {
    FILE *fIndiceProdutos = fopen(indiceProdutos, "rb");
    if (fIndiceProdutos == NULL) {
        printf("Erro ao abrir o arquivo de indice.\n");
        return -1;
    }

    fseek(fIndiceProdutos, 0, SEEK_END);
    long file_size = ftell(fIndiceProdutos);
    int num_records = file_size / sizeof(IndiceProduto);
    rewind(fIndiceProdutos);

    IndiceProduto indice;
    int left = 0, right = num_records - 1;
    long start_position = 0;

    while (left <= right) {
        int mid = (left + right) / 2;
        fseek(fIndiceProdutos, mid * sizeof(IndiceProduto), SEEK_SET);
        fread(&indice, sizeof(IndiceProduto), 1, fIndiceProdutos);

        if (indice.product_id == product_id) {
            start_position = indice.file_position;
            break;
        } else if (indice.product_id < product_id) {
            start_position = indice.file_position;
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    fclose(fIndiceProdutos);
    return start_position;
}

void printDadosProdutosComIndice(int product_id) {
    FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
    FILE *fExtensionArea = fopen("extension.bin", "rb");

    if (fDadosProdutos == NULL) {
        printf("Erro ao abrir o arquivo de produtos.\n");
        return;
    }

    DadoProduto produto;
    long pos = buscarNoIndiceProduto(product_id);

    if (pos == -1) {
        printf("Produto nao encontrado.\n");
        fclose(fDadosProdutos);
        if (fExtensionArea != NULL) fclose(fExtensionArea);
        return;
    }

    fseek(fDadosProdutos, pos, SEEK_SET);
    bool found = false;

    while (fread(&produto, sizeof(DadoProduto), 1, fDadosProdutos)) {
        if (!produto.deleted && produto.product_id == product_id) {
            printf("Product ID: %d | Category ID: %llu | Category Code: %s | Brand: %s | Price: %.2f\n",
                   produto.product_id, produto.category_id, produto.category_code, produto.brand, produto.price);
            found = true;
            break;
        } else if (produto.product_id > product_id) {
            break;
        }
    }

    if (!found && fExtensionArea != NULL) {
        rewind(fExtensionArea);

        while (fread(&produto, sizeof(DadoProduto), 1, fExtensionArea)) {
            if (produto.product_id == product_id) {
                printf("Product ID: %d | Category ID: %llu | Category Code: %s | Brand: %s | Price: %.2f\n",
                       produto.product_id, produto.category_id, produto.category_code, produto.brand, produto.price);
                found = true;
                break;
            }
        }

        fclose(fExtensionArea);
    }

    if (!found) {
        printf("Produto nao encontrado.\n");
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
    newAcesso->incremental_key = getNextIncrementalKey();

    FILE *fDadosAcessos = fopen(dadosAcesso, "ab");
    if (fDadosAcessos == NULL) {
        printf("Erro ao abrir o arquivo de acessos.\n");
        return;
    }

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

    while (fgets(line, sizeof(line), csv_file)) {
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
    FILE *fExtensionArea = fopen("extension.bin", "rb");

    if (fDadosProdutos == NULL) {
        printf("Erro ao abrir o arquivo de produtos.\n");
        return;
    }

    DadoProduto produto;
    printf("Listando produtos:\n");
    printf("--------------------------------------------------------------\n");
    printf("| Product ID | Category ID | Category Code   | Brand   | Price |\n");
    printf("--------------------------------------------------------------\n");

    while (fread(&produto, sizeof(DadoProduto), 1, fDadosProdutos)) {
        if (!produto.deleted) {
            printf("| %d | %llu | %s | %s | %.2f |\n",
                   produto.product_id,
                   produto.category_id,
                   produto.category_code,
                   produto.brand,
                   produto.price);
        }
    }

    if (fExtensionArea != NULL) {
        while (fread(&produto, sizeof(DadoProduto), 1, fExtensionArea)) {
            printf("| %d | %llu | %s | %s | %.2f |\n",
                   produto.product_id,
                   produto.category_id,
                   produto.category_code,
                   produto.brand,
                   produto.price);
        }
        fclose(fExtensionArea);
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
        if (!acesso.deleted) {
            printf("| %d | %d | %s | %s | %s |\n",
                acesso.incremental_key,
                acesso.user_id,
                acesso.user_session,
                acesso.event_time,
                acesso.event_type);
        }
    }

    printf("--------------------------------------------------------------------------------------\n");

    fclose(fDadosAcessos);
}


int main() {
    bool x = true;

    while (x) {
        printf("\nEscolha as opcoes:\n");
        printf("1  - Criar os arquivos de dados\n");
        printf("2  - Mostrar todos os dados dos produtos\n");
        printf("3  - Mostrar todos os dados dos acessos\n");
        printf("4  - Criar Arquivo de indice produto\n");
        printf("5  - Procurar produto por ID\n");
        printf("6  - Criar indice acesso\n");
        printf("7  - Procurar indice acesso\n");
        printf("8  - Inserir produto\n");
        printf("9  - Excluir produto\n");
        printf("10 - Reorganizar produtos\n");
        printf("11 - Inserir acesso\n");
        printf("12 - Excluir acesso\n");
        printf("13 - Reorganizar acessos\n");
        printf("14 - Criar B-Tree para indices de produtos\n");
        printf("15 - Mostrar estrutura B-tree\n");
        printf("16 - Procurar registro utilizando B-tree\n");
        printf("17 - Criar tabela Hash para os acessos\n");
        printf("18 - Procurar utilizando a tabela Hash\n");
        printf("20 - Sair\n");

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
                printf("Indices de acessos criados com sucesso.\n");
                break;

            case 7: {
                int incremental_key;
                printf("Digite o id a ser procurado: ");
                scanf("%d", &incremental_key);
                buscarAcesso(incremental_key);
                break;
            }

            case 8: {
                DadoProduto produto; 
                produto.deleted = false;

                printf("Insira o ID do produto: ");
                scanf("%d", &produto.product_id);

                printf("Insira o ID da categoria: ");
                scanf("%llu", &produto.category_id);

                printf("Insira o código da categoria: ");
                scanf("%s", produto.category_code);

                printf("Insira a marca do produto: ");
                scanf("%s", produto.brand);

                printf("Insira o preço do produto: ");
                scanf("%f", &produto.price);

                insertProductWithExtension(&produto);

                printf("Produto inserido com sucesso.\n");
                break;
            }

            case 9: {
                int id;
                printf("Digite o codigo id a ser removido: ");
                scanf("%d", &id);
                removeProduct(id);
                break;
            }

            case 10:
                reorganizeFile();
                break;

            case 11: { // Insert access
                DadoAcesso acesso;

                printf("Insira o ID do usuario: ");
                scanf("%d", &acesso.user_id);
                
                printf("Digite a user session:");
                scanf("%s", acesso.user_session);

                printf("Insira a data do acesso: ");
                scanf("%s", acesso.event_time);

                printf("Insira o tipo do evento: ");
                scanf("%s", &acesso.event_type);

                insertAcesso(&acesso); // Function to insert access

                printf("Acesso inserido com sucesso.\n");
                break;
            }

            case 12: { // Delete access
                int id;
                printf("Digite o codigo id do acesso a ser removido: ");
                scanf("%d", &id);
                deleteAcesso(id); // Function to remove access
                break;
            }

            case 13: { // Reorganize access file
                reorganizeAcessoFile(); // Function to reorganize access file
                printf("Arquivo de acessos reorganizado com sucesso.\n");
                break;
            }
            
            case 14: {
                criarBtree();
                break;
            }

            case 15:{
                printf("Árvore B:\n");
                traversal(root);
                break;
            }

            case 16:{
                printf("Digite o id do produto: \n");
                int searchKey;
                scanf("%d", &searchKey);
                long file_position;
                search(searchKey, &file_position, root);

                if (file_position != -1) {
                    printf("Produto encontrado: Chave: %d, Endereço: %ld\n", searchKey, file_position);
                } else {
                    printf("Produto não encontrado.\n");
                }

                FILE *fDadosProdutos = fopen(dadosProdutos, "rb");
                if (fDadosProdutos == NULL) {
                    printf("Erro ao abrir o arquivo de produtos.\n");
                    return;
                }
                fseek(fDadosProdutos, file_position, SEEK_SET);

                DadoProduto produto;
                while (fread(&produto, sizeof(DadoProduto), 1, fDadosProdutos)) {
                    if (produto.product_id == searchKey) {
                        printf("Product ID: %d | Category ID: %llu | Category Code: %s | Brand: %s | Price: %.2f\n",
                            produto.product_id, produto.category_id, produto.category_code, produto.brand, produto.price);
                        break;
                    } else {
                        printf("Produto nao encontrado.\n");
                        break;
                    }
                }

                fclose(fDadosProdutos);
                break;
            }
            
            case 17:{
                criarHashTableAcessos();
                break;
            }

            case 18:{
                printf("Digite a chave: ");
                int incremental_key;
                scanf("%d", &incremental_key);
                long position_file = searchInHashTable(incremental_key);

                FILE *fDadosAcessos = fopen(dadosAcesso, "rb");
                DadoAcesso acesso;
                fseek(fDadosAcessos, position_file, SEEK_SET);
                while (fread(&acesso, sizeof(DadoAcesso), 1, fDadosAcessos)) {
                    if (acesso.incremental_key == incremental_key) {
                        printf("Acesso encontrado: Incremental Key: %d, User ID: %d, Session: %s, Event Time: %s, Event Type: %s\n",
                            acesso.incremental_key, acesso.user_id, acesso.user_session, acesso.event_time, acesso.event_type);
                        break;
                    } else if (acesso.incremental_key > incremental_key) {
                        printf("Acesso nao encontrado.\n");
                        break;
                    }
                }

                fclose(fDadosAcessos);
                break;
            }
            case 20:
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
