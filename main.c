#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>


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

//定义表的内存结构，
//定义一个页的大小，和操作系统页的大小一致，4KB
#define PAGE_SIZE 4096
//一个表能有多少页
#define TABLE_MAX_PAGES 100
//一个表的最大行数,以及一页的最大行数
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

//表结构体
typedef struct{
    uint32_t num_rows; //表中当前的行数
    void* pages[TABLE_MAX_PAGES]; //指向页的指针数组，每个页都是一个连续的内存块
}Table;

//计算某一行的具体地址
//根据行号返回其在那个页的哪个位置的指针
void* row_slot(Table* table, uint32_t row_num){
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    if(page == NULL){
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

//初始化表
Table* new_table(){
    Table* table = (Table*)malloc(sizeof(Table));
    table->num_rows = 0;
    for(int i = 0; i<TABLE_MAX_PAGES; i++){
        table->pages[i] = NULL;
    }
    return table;
}

//释放表所占的内存
void free_table(Table* table){
    for(int i = 0; i<TABLE_MAX_PAGES; i++){
        free(table->pages[i]);
    }
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
        free_table(table);    //退出前释放表内存
        exit(EXIT_SUCCESS);
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
    if(table->num_rows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    //计算出新行应该放在哪里
    void* destination = row_slot(table, table->num_rows);
    //把行数据序列化到这个位置
    serialize_row(row_to_insert, destination);
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}

//真正的select执行逻辑
ExecuteResult execute_select(Statement* statement, Table* table){
    Row row;
    for(int i = 0; i < table->num_rows; i++){
        //计算出第i行数据的位置
        void* source = row_slot(table, i);
        //从这个位置反序列化出行数据Row
        deserialize_row(source, &row);
        //打印数据
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
    }
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
    // 初始化一个表
    Table* table = new_table();
    InputBuffer* input_buffer = new_input_buffer();

    // // ========== 添加调试信息打印 ==========
    // printf("\n=== 数据结构调试信息 ===\n");
    // printf("Size of Row: %zu bytes\n", sizeof(Row));
    // printf("ID offset: %zu, size: %u\n", offsetof(Row, id), ID_SIZE);
    // printf("Username offset: %zu, size: %u\n", offsetof(Row, username), USERNAME_SIZE);
    // printf("Email offset: %zu, size: %u\n", offsetof(Row, email), EMAIL_SIZE);
    
    // // 计算并显示页和行信息
    // printf("\n=== 存储结构信息 ===\n");
    // printf("Page size: %d bytes\n", PAGE_SIZE);
    // printf("Rows per page: %u\n", ROWS_PER_PAGE);
    // printf("Max rows: %u\n", TABLE_MAX_ROWS);
    // printf("========================\n\n");


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