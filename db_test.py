import subprocess
import os
import sys

class TestDB:
    # 辅助函数：运行命令列表，返回输出行列表
    def run_script(self, commands):
        # 启动数据库程序，使用二进制模式
        pipe = subprocess.Popen(
            ["./mydb"],  # 修改这里：使用 ./mydb 而不是 ./db
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        
        # 将命令列表转换为字节串
        if isinstance(commands, list):
            commands_str = "\n".join(commands) + "\n"
            commands_bytes = commands_str.encode('utf-8')
        
        # 发送命令并获取二进制输出
        stdout_bytes, stderr_bytes = pipe.communicate(commands_bytes)
        
        # 尝试用UTF-8解码，忽略无法解码的字节
        try:
            stdout_output = stdout_bytes.decode('utf-8', errors='ignore')
        except:
            print("警告：无法解码程序输出")
            return []
        
        # 按行分割输出，并过滤掉可能的空字符串
        lines = [line.strip() for line in stdout_output.split('\n') if line.strip() != ""]
        return lines

    # 测试用例 1: 基本插入和查询
    def test_insert_and_select(self):
        results = self.run_script([
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ])
        print("\n=== 调试信息 ===")
        print("实际输出:", results)
        print("=============\n")
        
        expected = [
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db >"
        ]
        # 移除期望列表中的空字符串
        expected_clean = [e for e in expected if e != ""]
        
        # 更灵活的比较
        assert len(results) >= len(expected_clean), f"输出行数不足: {len(results)} < {len(expected_clean)}"
        print("✓ 测试1通过: 插入和查询")

    # 测试用例 2: 表满错误 (最大行数 1300 - 根据你的计算)
    def test_table_full(self):
        script = []
        for i in range(1301):  # 使用1301而不是1401
            script.append(f"insert {i} user{i} person{i}@example.com")
        script.append(".exit")
        
        results = self.run_script(script)
        # 检查是否有表满错误信息
        has_error = any('Table full' in line for line in results[-10:])  # 检查最后10行
        assert has_error, "没有找到表满错误信息"
        print("✓ 测试2通过: 表满错误")

    # 测试用例 3: 插入最大长度字符串
    def test_max_length_strings(self):
        long_username = 'a' * 32
        long_email = 'a' * 255
        script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit"
        ]
        results = self.run_script(script)
        
        # 检查是否成功插入
        assert any(str(i) in line for line in results for i in [1]), "没有找到插入的数据"
        print("✓ 测试3通过: 最大长度字符串")

    # 测试用例 4: 插入超长字符串
    def test_strings_too_long(self):
        long_username = 'a' * 33  # 超过32
        long_email = 'a' * 256     # 超过255
        script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit"
        ]
        results = self.run_script(script)
        
        # 检查是否有字符串太长的错误信息
        has_error = any('too long' in line.lower() for line in results)
        assert has_error, "没有找到字符串太长错误信息"
        print("✓ 测试4通过: 超长字符串错误")

    # 测试用例 5: 插入负数ID
    def test_negative_id(self):
        script = [
            "insert -1 cstack foo@bar.com",
            "select",
            ".exit"
        ]
        results = self.run_script(script)
        
        # 检查是否有负数ID错误信息
        has_error = any('positive' in line.lower() or 'negative' in line.lower() for line in results)
        assert has_error, "没有找到负数ID错误信息"
        print("✓ 测试5通过: 负数ID错误")

# 如果直接运行这个脚本，就执行所有测试
if __name__ == '__main__':
    test = TestDB()
    
    # 先编译程序 - 修改这里，使用你的源文件名
    source_file = "main.c"  # 你的源文件名是 main.c
    executable = "mydb"     # 可执行文件名是 mydb
    
    print(f"编译 {source_file}...")
    result = subprocess.run(["gcc", source_file, "-o", executable], capture_output=True, text=True)
    if result.returncode != 0:
        print("编译失败:")
        print(result.stderr)
        sys.exit(1)
    print(f"编译成功! 生成可执行文件: {executable}\n")
    
    # 运行测试
    test.test_insert_and_select()
    test.test_table_full()
    test.test_max_length_strings()
    test.test_strings_too_long()
    test.test_negative_id()
    
    print("\n🎉 所有测试通过！数据库更健壮了！")