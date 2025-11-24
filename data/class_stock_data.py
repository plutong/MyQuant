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
# K线数据默认参数
start_date = '2020-01-01' # 默认开始日期
end_date = datetime.today().strftime('%Y-%m-%d') # 默认结束日期
k_type = 1 # 读取日K线

#%% 单一股票数据类
class SingleStockData:
    def __init__(self, 
                 db_path: str = database_path,
                 code: str = '000001',
                 start_date : str = start_date,
                 end_date : str = end_date,
                 name: str = '',
                 exchange: str = ''):
        """初始化股票数据类"""
        self.db_path = db_path
        self.code = code
        self.name = name
        self.exchange = exchange
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
            query = f"SELECT * FROM  stock WHERE 股票代码 = '{self.code}'"
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
        
    def get_stock_adata(self) -> pd.DataFrame:
        print(f"获取股票 {self.code} 的 AData 对象")
        # 获取股票基本信息
        df_info = pd.DataFrame(data={'股票代码': [self.code], 
                                     '股票名称': [self.name], 
                                     '交易所': [self.exchange]})
        # 获取股票市值数据
        df_volume = ad.stock.info.get_stock_shares(stock_code=self.code, is_history=False)
        df_volume = df_volume[['stock_code', 'total_shares']]
        df_volume.rename(columns={'stock_code':'股票代码', 'total_shares':'总股本'}, inplace=True)
        # 获取股票交易数据
        df_trade = ad.stock.market.get_market(stock_code=self.code,
                                              start_date=self.)
        
        
    
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
                df_info = ad.stock_info_a_code_name()
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
