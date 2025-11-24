# %% 包
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import adata as ad
import sys
import os

# %% 全局变量
database_path = '../AShare.db'
database_table = 'stock'
col_list = {
        '日期': 'TEXT',
        '股票代码': 'TEXT',
        '股票名称': 'TEXT',
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
# K线数据默认参数
start_date = '2020-01-01' # 默认开始日期
end_date = datetime.today().strftime('%Y-%m-%d') # 默认结束日期
k_type = 1 # 读取日K线

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
        print("创建空数据库文件...")
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
        print("初始化数据库...")
        if not self.exists():
            print("数据库文件不存在，正在创建...")
            self.create_empty_database()
            return
        if not self.validate_schema():
            print("数据库表结构不匹配！")
            self.delete_database()
            self.create_empty_database()   

#%% 单一股票数据类
class SingleStockData:
    def __init__(self, 
                 db_path: str = database_path,
                 code: str = '000001'):
        """初始化股票数据类"""
        self.db_path = db_path
        self.code = code
        try:
            self.conn = sqlite3.connect(self.db_path)
        except:
            print(f"无法连接到数据库 {self.db_path}")
            sys.exit()
    
    def close(self):
        print("关闭数据库连接")
        if self.conn:
            self.conn.close()
    
    def read_stock_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        print(f"从数据库读取{self.code}股票数据")
        if not self.conn:
            self.connect()
        
        try:
            query = f"SELECT * FROM stock WHERE 股票代码 = '{self.code}'"
        except :
            self.init_database()
        
        if start_date:
            query += f" AND 日期 >= '{start_date}'"
        if end_date:
            query += f" AND 日期 <= '{end_date}'"
        
        query += " ORDER BY 日期 DESC"
        
        return pd.read_sql(query, self.conn)
    
    def update_stock_data(self, name: str, 
                         start_date: str = None, end_date: str = None) -> bool:
        """更新单个股票数据"""
        if not self.conn:
            self.connect()
        
        if not start_date:
            start_date = (datetime.strptime(self.read_stock_data()['日期'].max(),'%Y-%m-%d') + timedelta(days=1)).strftime(date_read_format)
        if not end_date:
            end_date = datetime.today().strftime(date_read_format)
        
        try:
            print(f"正在更新股票 {self.code} 的数据...")
            df = ak.stock_zh_a_hist(symbol=self.code, period="daily", 
                                   start_date=start_date, end_date=end_date, adjust="qfq")
            df['股票名称'] = name
            
            df.to_sql('stock', self.conn, if_exists='append', index=False)
            self.conn.commit()
            print(f"股票 {symbol} 数据更新完成。")
            return True
            
        except Exception as e:
            print(f"更新股票 {symbol} 数据时出错: {e}")
            return False
    
    def write_stock_data(self, symbol: str, name: str, df: pd.DataFrame) -> bool:
        """写入股票数据到数据库"""
        if not self.conn:
            self.connect()
        
        try:
            df['股票名称'] = name
            df.to_sql('stock', self.conn, if_exists='append', index=False)
            self.conn.commit()
            print(f"股票 {symbol} 数据写入完成。")
            return True
            
        except Exception as e:
            print(f"写入股票 {symbol} 数据时出错: {e}")
            return False
    
    def init_database(self):
        """初始化数据库表并检查股票数据"""
        cursor = self.conn.cursor()
        
        # 检查该股票代码是否存在数据
        check_sql = f"SELECT COUNT(*) FROM stock WHERE 股票代码 = '{self.code}'"
        count = cursor.execute(check_sql).fetchone()[0]
        
        if count == 0:
            print(f"数据库中不存在数据，正在加载历史数据...")
            
            try:
                # 获取股票名称
                df_info = ak.stock_info_a_code_name()
                stock_name = df_info[df_info['code'] == self.code]['name'].values
                
                if len(stock_name) == 0:
                    print(f"无法找到股票代码 {self.code} 的信息")
                    return
                
                stock_name = stock_name[0]
                
                # 从2020-01-01开始加载历史数据
                start_date = '20200101'
                end_date = datetime.today().strftime('%Y%m%d')
                
                print(f"正在加载股票 {self.code}({stock_name}) 从 {start_date} 到 {end_date} 的数据...")
                df = ak.stock_zh_a_hist(symbol=self.code, period="daily", 
                                    start_date=start_date, end_date=end_date, adjust="qfq")
                df['股票名称'] = stock_name
                
                df.to_sql('stock', self.conn, if_exists='append', index=False)
                self.conn.commit()
                print(f"股票 {self.code} 历史数据加载完成，共 {len(df)} 条记录")
                
            except Exception as e:
                print(f"加载股票 {self.code} 历史数据时出错: {e}")
        else:
            print(f"股票代码 {self.code} 已存在 {count} 条记录")


# %% 函数
def load_stock_data(stock_list: list[str] = ak.stock_info_a_code_name()['code'].tolist(), 
                    stock_name_list: list[str] = ak.stock_info_a_code_name()['name'].tolist(), 
                    start_date: str = (datetime.today().replace(year=datetime.today().year-5)).strftime('%Y%m%d'),
                    end_date: str = datetime.today().strftime('%Y%m%d'),
                    conn = sqlite3.connect('..\AShare.db')):
    print("="*20)
    print("检查数据库是否存在并且结构正确")
    db = 
    
# %%
if __name__ == '__main__':
    load_stock_data()
    
# %%
