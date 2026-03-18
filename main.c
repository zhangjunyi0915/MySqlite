#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>


typedef struct{
    char* buffer;            //保存字符串的地址
    size_t buffer_length;    //内存分配的大小
    ssize_t input_length;    //输入的字符串长度，ssize_t是有符号的size_t类型，允许表示负数，通常用于表示错误情况
}InputBuffer;

InputBuffer* new_input_buffer(){
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void print_prompt(){
    printf("db > ");
}

void read_input(InputBuffer* input_buffer){
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if(bytes_read <= 0){
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    // 忽略掉最后的回车换行符 '\n' (将它替换为字符串结束符 '\0')
    input_buffer->input_length = bytes_read -1;
    input_buffer->buffer[bytes_read - 1] = 0;

}

void close_input_buffer(InputBuffer* input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

//定义表的内存结构，
//定义一个页的大小，和操作系统页的大小一致，4KB
#define PAGE_SIZE 4096
//一个表能有多少页
#define TABLE_MAX_PAGES 100

//定义数据行的结构
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1]; // +1 是为了存储字符串结束符 '\0'
    char email[COLUMN_EMAIL_SIZE + 1];       // +1 是为了存储字符串结束符 '\0'
}Row;

//序列化把Row结构体转化为紧凑的字节流
//定义每个字段在序列化字节流中所占空间和偏移量
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
//计算偏移量
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

//序列化函数：把Row结构体的内容写入到一个连续的内存地址（destination）
void serialize_row(Row* source, void* destination){
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

//反序列化从连续地址数据并填充到Row结构体
void deserialize_row(void* source, Row* destinantion){
    memcpy(&(destinantion->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destinantion->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destinantion->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

//Pager负责管理页，它可以从磁盘读取页也可以向磁盘写入页
typedef struct{
    FILE* file_descriptor;    //文件指针
    uint32_t file_length;      //文件长度
    uint32_t num_pages;        //文件中页的数量
    void* pages[TABLE_MAX_PAGES];  //内存中的页指针数组
}Pager;

typedef struct{
    uint32_t root_page_num; //根节点所在页的页号
    Pager* pager;
}Table;

//定义游标
typedef struct{
    Table* table;
    uint32_t page_num;
    uint32_t cell_num; 
    bool end_of_table; //当游标到达表的末尾时为true
}Cursor;

//定义节点的类型
typedef enum{
    NODE_INTERNASL,
    NODE_LEAF
}NodeType;

//定义公共节点的头部布局，定义头部个字段的大小
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
//定义头部个字段的偏移量
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;

//叶子节点在公共头部数据之外还需要存储一些特定于叶子节点的数据
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

//叶子节点存储数据的布局，数据体紧跟在头部之后
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;


//偏移的本质ptr + n  =  地址 + n * sizeof(*ptr)
//获取一个节点中存储单元的（cell）数量
uint32_t* leaf_node_num_cells(void* node){
    return (uint32_t*)((char*)node + LEAF_NODE_NUM_CELLS_OFFSET);
}

//获取一个节点中第N个单元的数据
void* leaf_node_cell(void* node, uint32_t cell_num){
    return (void*)((char*)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE);
}

//获取一个单元中的key的地址
uint32_t* leaf_node_key(void* node, uint32_t cell_num){
    return (uint32_t*)leaf_node_cell(node, cell_num);
}

//获取一个单元中Value的地址
void* leaf_node_value(void* node, uint32_t cell_num){
    return (void*)((char*)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE);
}

// 获取节点类型
NodeType get_node_type(void* node){
    uint8_t value = *((uint32_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

//设置节点的类型
void set_node_type(void* node, NodeType type){
    uint32_t value = type;
    *((uint32_t*)(node + NODE_TYPE_OFFSET)) = value;
}

//初始化一个叶子节点
void initialize_leaf_node(void* node){
    set_node_type(node, NODE_LEAF);
    *leaf_node_num_cells(node) = 0; 
}

//打印常量调试
void print_constants(){
    printf("ROW_SIZE:%d\n", ROW_SIZE);
    printf("PAGE_SIZE:%d\n", PAGE_SIZE);
    printf("ID offset: %zu, size: %u\n", offsetof(Row, id), ID_SIZE);
    printf("Username offset: %zu, size: %u\n", offsetof(Row, username), USERNAME_SIZE);
    printf("Email offset: %zu, size: %u\n", offsetof(Row, email), EMAIL_SIZE);
    printf("COMMON_NODE_HEADER_SIZE:%d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE:%d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE:%d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS:%d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS:%d\n", LEAF_NODE_MAX_CELLS);
}

//打印树结构
void print_leaf_node(void* node){
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("leaf (size %d)\n ", num_cells);
    for(int i = 0; i < num_cells; i++){
        uint32_t key = *leaf_node_key(node, i);
        printf("  - %d : %d\n", i, key);
    }
}

//获取指定页面，如果页面不在缓存区就从磁盘读取
void* get_page(Pager* pager, uint32_t page_num){
    if(page_num > TABLE_MAX_PAGES){
        printf("tried to fecth page number out of bounds, %d > %d \n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    //如果缓存中没有这一页需要从磁盘中加载
    if(pager->pages[page_num] == NULL){
        //分配4KB的内存来存储这一页
        void* page = pager->pages[page_num] = malloc(PAGE_SIZE);
        //计算文件中的完整页数
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        if(pager->file_length % PAGE_SIZE){
            num_pages +=1;
        }

        if(page_num < num_pages){
            //请求页在文件范围内，从文件中读取
            fseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            size_t bytes_read = fread(page, PAGE_SIZE, 1, pager->file_descriptor);
            if(bytes_read == -1){
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }else{
            //请求页在文件范围外，初始化为0
            memset(page, 0, PAGE_SIZE);
        }
        //将新加载的页存入缓存
        pager->pages[page_num] = page;
        if(page_num >= pager->num_pages){
            pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];

}


void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value){
    void* node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if(num_cells >= LEAF_NODE_MAX_CELLS){
        //节点已满，无法插入
        printf("Error: Leaf node full.\n");
        exit(EXIT_FAILURE);
    }
    if(cursor->cell_num < num_cells){
        //需要为新单元腾出空间，移动现有单元
        for(uint32_t i = num_cells; i > cursor->cell_num; i--){
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }
    //插入新的单元
    *leaf_node_num_cells(node) += 1;
    *leaf_node_key(node, cursor->cell_num) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}



//打开数据库文件并初始化Pager
Pager* pager_open(const char* filename){
    FILE* file_descriptor = fopen(filename, "rb+");
    // O_RDWR: 读写模式 | O_CREAT: 文件不存在则创建
    // S_IWUSR: 用户可写 | S_IRUSR: 用户可读
    if(file_descriptor == NULL){
        // 如果文件打开失败（比如不存在），就以写模式创建一个新文件
        file_descriptor = fopen(filename, "wb+");
        if(file_descriptor == NULL){
            printf("Unable to open file %s\n", filename);
            exit(EXIT_FAILURE);
        }

    }
    //获取文件的大小
    //SEEK_END: 从文件末尾开始计算偏移量，0表示文件末尾
    //SEEK_SET: 从文件开头开始计算偏移量，0表示文件开头
    fseek(file_descriptor, 0, SEEK_END);
    uint32_t file_length = ftell(file_descriptor);  // 返回当前距离文件开头的字节数
    Pager* pager = (Pager*)malloc(sizeof(Pager));
    pager->file_descriptor = file_descriptor;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);
    if(file_length % PAGE_SIZE != 0){
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }
    for(int i = 0; i < TABLE_MAX_PAGES; i++){
        pager->pages[i] = NULL;
    }

    return pager;
}



//将指定页刷到磁盘
void* pager_flush(Pager* pager, uint32_t page_num){
    if(pager->pages[page_num] == NULL){
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    //定位到文件中该页的起始位置
    fseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    size_t bytes_written = fwrite(pager->pages[page_num], PAGE_SIZE, 1, pager->file_descriptor);
    if (bytes_written != 1) {
        printf("Error writing to file\n");
        exit(EXIT_FAILURE);
    }
}


//一个表的最大行数,以及一页的最大行数
//const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
//const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
//表结构体


//创建一个指向表头的游标
Cursor* table_start(Table* table){
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0); //如果根节点没有任何单元格，游标直接指向末尾
    return cursor;
}
//创建一个指向表尾的游标
Cursor* table_end(Table* table){
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_num = num_cells; //游标指向最后一个单元格
    cursor->end_of_table = true; //游标直接指向末尾
    return cursor;
}

//通过游标获取当前行的地址
void* cursor_value(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}
// 移动游标到下一行
void cursor_advance(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);
    cursor->cell_num += 1;
    if(cursor->cell_num >= *leaf_node_num_cells(node)){
        cursor->end_of_table = true;
    }
}


//打开数据库（初始化table和pager）
Table* db_open(const char* filename){
    Pager* pager = pager_open(filename);
    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;
    //如果数据库文件为空
    if(pager->num_pages ==0){
        void* root_node = get_page(pager,0);
        initialize_leaf_node(root_node);

    }
    return table;
}

//关闭数据库，释放资源
void db_close(Table* table){
    Pager* pager = table->pager;
    //将所有完整的页刷到磁盘
    for(int i = 0; i < pager->num_pages; i++){
        if(pager->pages[i] == NULL){
            continue;
        }
        pager_flush(pager, i);
    }
    //关闭文件和释放pager和Table内存
    int result = fclose(pager->file_descriptor);
    if(result == -1){
        printf("Error cloing file.\n");
        exit(EXIT_FAILURE);
    }
    for(int i = 0; i < TABLE_MAX_PAGES; i++){
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
        
    }
    free(pager);
    free(table);
}




//定义状态吗
typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}MetaCommandResult;

typedef enum{
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR, //语法错误
    PREPARE_STRING_TOO_LONG, //输入的字符太长超过了标准的限制
    PREPARE_NEGATIVE_ID, //id不能为负数
    PREPARE_UNRECOGNIZED_STATEMENT
}PrepareResult;

//定义支持的语句
typedef enum{
    STATEMENT_INSERT,
    STATEMENT_SELECT
}StatementType;

//定义执行结果
typedef enum{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
}ExecuteResult;




// 3. 语句(Statement)结构体，用于在“编译器”和“虚拟机”之间传递信息
typedef struct{
    StatementType type;
    Row row_to_insert; // 仅在插入语句中使用
}Statement;

// 4. 处理元命令 (以 . 开头的命令)
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table){
    if(strcmp(input_buffer->buffer, ".exit") == 0){
        close_input_buffer(input_buffer);
        db_close(table);    //退出前释放表内存
        exit(EXIT_SUCCESS);
    }else if (strcmp(input_buffer->buffer, ".constants") == 0) { // 👈【新增分支】
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }else if (strcmp(input_buffer->buffer, ".btree") == 0) { // 👈【新增分支】
        printf("Tree:\n");
        void* node = get_page(table->pager, 0);
        print_leaf_node(node);
        return META_COMMAND_SUCCESS;
    }else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

//更新Insert解析器，需要从字符串解析出id,username和email,并且把它们存储在Statement结构体中
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement){
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    //检查是否缺少了元素
    if(id_string == NULL || username == NULL || email == NULL){
        return PREPARE_SYNTAX_ERROR;
    }
    //把id字符串转化为整数
    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID; // 新增错误码
      }
    //检查字符串长度
    if(strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    //把解析出来的值存在Statement结构体中
    statement->row_to_insert.id = id;
    strncpy(statement->row_to_insert.username, username, COLUMN_USERNAME_SIZE);
    strncpy(statement->row_to_insert.email, email, COLUMN_EMAIL_SIZE);

    return PREPARE_SUCCESS;
}


// 编译前端，解析sql字符
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement){
    if(strncmp(input_buffer->buffer, "insert",6) == 0){
        return prepare_insert(input_buffer,statement);
    }
    if(strncmp(input_buffer->buffer, "select", 6) == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

//真正执行insert的逻辑
ExecuteResult execute_insert(Statement* statement, Table* table){
    void* node = get_page(table->pager, table->root_page_num);
    if((*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)){
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    //创建出指向末尾的游标
    Cursor* cursor = table_end(table);
    //把行数据序列化到这个位置
    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
    free(cursor);
    return EXECUTE_SUCCESS;
}

//真正的select执行逻辑
ExecuteResult execute_select(Statement* statement, Table* table){
    //创建出指向表头的游标
    Cursor* cursor = table_start(table);
    Row row;
    while(!cursor->end_of_table){
        deserialize_row(cursor_value(cursor), &row);
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
        // 移动游标到下一行
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

//虚拟机后端执行解析好的Stetement
ExecuteResult execute_statement(Statement* statement, Table* table){
    switch(statement->type){
        case STATEMENT_INSERT:
            return execute_insert(statement, table);

        case STATEMENT_SELECT:
            return execute_select(statement, table);


    }
}



int main(int argc, char *argv[]){
    // 运行程序时需要指定数据库文件名
    if(argc < 2){
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }
    char* filename = argv[1];
    Table* table = db_open(filename);
    InputBuffer* input_buffer = new_input_buffer();




    while(true){
        print_prompt();
        read_input(input_buffer);

        // 检查输入的是否是元命令
        if(input_buffer->buffer[0] == '.'){
            switch(do_meta_command(input_buffer, table)){
                case META_COMMAND_SUCCESS:
                    continue;  // 如果成功，跳过本次循环的剩余部分，直接开始下一次循环
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }
        //如果不是元命令就当sql命令处理
        Statement statement;
        switch(prepare_statement(input_buffer, &statement)){
            case PREPARE_SUCCESS:
                break;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long.\n");
                continue;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer);
                continue;
        }

        switch(execute_statement(&statement, table)){
            case EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Error: Table full.\n");
                break;
        }
        
    }

}