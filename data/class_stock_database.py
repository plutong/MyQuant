# %% 包
import sqlite3
import os

# %% 全局变量
database_path = '../AShare.db'
database_table = 'stock'
col_list = {
        '日期': 'TEXT',
        '股票代码': 'TEXT',
        '股票名称': 'TEXT',
        '交易所': 'TEXT',
        '一级行业': 'TEXT',
        '二级行业': 'TEXT',
        '市值': 'REAL',
        '开盘': 'REAL',
        '收盘': 'REAL',
        '最高': 'REAL',
        '最低': 'REAL',
        '成交量': 'REAL',
        '成交额': 'REAL',
        '振幅': 'REAL',
        '涨跌幅': 'REAL',
        '涨跌额': 'REAL',
        '换手率': 'REAL'
}
key_list = ['日期', '股票代码']

# %% 数据库类
class DatabaseManager:
    """
    管理 SQLite 数据库文件与表结构：
    - 记录数据库地址
    - 检查/创建空数据库（包含 stock 表，列与 col_list 对齐）
    - 校验表结构是否符合期望
    - 删除数据库文件
    """
    def __init__(self, 
                 db_path: str = database_path, 
                 db_name: str = database_table,
                 col_list = col_list,
                 key_list: list[str] = key_list):
        self.db_path = os.path.abspath(db_path)
        self.db_name = db_name
        self.db_col_list = col_list
        self.db_key_list = key_list

    def exists(self) -> bool:
        print(f"检查数据库文件是否存在于 {self.db_path} ...")
        return os.path.exists(self.db_path) and os.path.getsize(self.db_path) > 0

    def create_empty_database(self):
        print(f"在{self.db_path}创建空数据库文件...")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # 创建表格框架
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {self.db_name} ("
        # 添加列定义
        for col_name, col_type in self.db_col_list.items():
            if col_name in self.db_key_list: create_table_sql += f"{col_name} {col_type} NOT NULL, "
            else: create_table_sql += f"{col_name} {col_type}, "
        # 添加主键约束
        create_table_sql += "PRIMARY KEY ("
        create_table_sql += ", ".join(self.db_key_list)
        create_table_sql += ") );"
        cursor.execute(create_table_sql)
        conn.commit()
        conn.close()

    def validate_schema(self) -> bool:
        """验证 stock 表是否存在且列名、类型与 col_list 对应"""
        print("验证数据库表结构...")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("PRAGMA table_info('stock')")
            cols = cursor.fetchall()  # [(cid, name, type, notnull, dflt_value, pk), ...]
            if not cols:
                return False
            existing_names = sorted([c[1] for c in cols])
            expected_names = sorted([name for name,_ in self.db_col_list.items()])
            print( f'数据库列名:{existing_names}\n要求列名{expected_names}')
            # 简单类型匹配检查
            for c in cols:
                name = c[1]
                typ = (c[2] or '').upper()
                exp = self.db_col_list.get(name)
                if exp and exp != typ:
                    print(f"列 {name} 类型不匹配，期望 {exp}，实际 {typ}")
                    return False
            return True
        finally:
            conn.close()
    
    def delete_database(self):
        print("删除数据库文件...")
        if self.exists():
            os.remove(self.db_path)

    def init_database(self):
        """
        初始化数据库：
        - 若不存在则创建空数据库（包含 stock 表）
        - 若存在但表结构不匹配，默认抛错；传入 force=True 则删除并重建
        """
        print("数据库初始化检查...")
        if not self.exists():
            print("数据库文件不存在，正在创建...")
            self.create_empty_database()
            return
        if not self.validate_schema():
            print("数据库表结构不匹配！")
            self.delete_database()
            self.create_empty_database()   
    
# %%
if __name__ == '__main__':
    databse_test = DatabaseManager(db_path='../AShare_test.db')
    databse_test.init_database()
