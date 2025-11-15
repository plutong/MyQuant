# %% 包
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import akshare as ak
import sys

#%% 单一股票数据类
class SingleStockData:
    def __init__(self, 
                 db_path: str = '../AShare.db',
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
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
    
    def read_stock_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """从数据库读取单个股票数据"""
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
        
        query += " ORDER BY 日期"
        
        return pd.read_sql(query, self.conn)
    
    def update_stock_data(self, symbol: str, name: str, 
                         start_date: str = None, end_date: str = None) -> bool:
        """更新单个股票数据"""
        if not self.conn:
            self.connect()
        
        if not start_date:
            start_date = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
        if not end_date:
            end_date = datetime.today().strftime('%Y%m%d')
        
        try:
            print(f"正在更新股票 {symbol} 的数据...")
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
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
            print(f"数据库中不存在股票代码 {self.code} 的数据，正在加载历史数据...")
            
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
    for (symbol, name) in zip(stock_list, stock_name_list):
        try:
            print(f"正在获取股票 {symbol} 的数据...")
            # 获取单个股票的历史日线数据（这里使用前复权）
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            df['股票名称'] = name
            
            # 将数据写入数据库的 stock_daily 表
            df.to_sql('stock', conn, if_exists='append', index=False)
            print(f"股票 {symbol} 数据写入完成。")
            
        except Exception as e:
            print(f"获取或写入股票 {symbol} 数据时出错: {e}")
            # 出错了也不要停，继续下一个
            continue

# %%
if __name__ == '__main__':
    # 创建数据库
    print(f"{'='*20}创建数据库{'='*20}")
    conn = sqlite3.connect('AShare.db')
    cursor = conn.cursor()

    # 创建日线行情数据表
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock (
        日期 TEXT NOT NULL,
        股票代码 TEXT NOT NULL,
        股票名称 TEXT,
        开盘 REAL,
        收盘 REAL,
        最高 REAL,
        最低 REAL,
        成交量 REAL,
        成交额 REAL,
        振幅 REAL,
        涨跌幅 REAL,
        涨跌额 REAL,
        换手率 REAL,
        -- 设置 (symbol, trade_date) 为联合主键，防止数据重复
        PRIMARY KEY (日期, 股票代码)
    );
    """
    cursor.execute(create_table_sql)
    conn.commit() # 提交事务
    print("数据库初始化已完成")

    # 获取最新时间
    print(f"{'='*20}获取最新时间{'='*20}")
    get_latest_date_sql = """
    SELECT MAX(日期) AS 日期, 股票代码, MAX(日期) OVER () AS 最新日期
    FROM stock
    GROUP BY 股票代码
    """
    df_date = pd.read_sql(get_latest_date_sql, conn)
    ratio = len(df_date[df_date['日期']==df_date['最新日期']])/len(df_date)
    if len(df_date[df_date['日期']!=df_date['最新日期']]) > 0:
        print(f'部分股票数据没有最新数据，如下\n{df_date[df_date['日期']!=df_date['最新日期']].head()}')
    latest_date = (pd.to_datetime(df_date['最新日期'].iloc[0]) + timedelta(days=1)).strftime('%Y%m%d')
    print(f"从{latest_date}开始更新")
    # else: 
    #     raise Exception('超八成股票的最新更新时间不统一，请检查数据库')
    
    # 更新数据库
    print(f"{'='*20}更新股票数据库{'='*20}")
    df_stock = ak.stock_info_a_code_name()
    load_stock_data(stock_list= df_stock['code'].tolist(), 
                    stock_name_list = df_stock['name'].tolist(), 
                    start_date = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d'),
                    end_date = datetime.today().strftime('%Y%m%d'))
    # 关闭数据库连接
    conn.close()
    print("所有数据获取并写入完成！")
    
# %%
