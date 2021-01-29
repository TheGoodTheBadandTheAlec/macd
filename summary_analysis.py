import pandas as pd
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
import numpy as np
import pandas as pd
import pandasql as ps


#master data
df_csv = pd.read_csv('blank1.csv')
df_csv = df_csv.rename(columns={1: 'currency', 2: 'datetime_UTC_buy', 3: 'timestamp_UTC_buy', 4: 'price_USD_buy',5: 'data_key_buy', 6: 'analysis_key', 7: 'datetime_UTC_sell', 8: 'timestamp_UTC_sell',9: 'price_USD_sell', 10: 'data_key_sell', 11: 'profit', 12: 'analysis_key_detail'})
df_analysis_master_data = pd.DataFrame(df_csv)
df_analysis_master_data = df_analysis_master_data.drop_duplicates(subset=df_analysis_master_data.columns.difference(['analysis_key_detail']))
#summary
df_analysis_summary = df_analysis_master_data[['analysis_key', 'datetime_UTC_buy', 'timestamp_UTC_buy', 'profit', 'price_USD_buy', 'price_USD_sell']]

#df_analysis_summary = df_analysis_master_data1.groupby('analysis_key').agg({'profit': ['count', 'sum', 'mean'], 'price_USD_buy': ['sum', 'mean'], 'price_USD_sell': ['sum', 'mean']})
#df_analysis_summary = df_analysis_summary.sort_index(ascending=False)



query = """
SELECT 
analysis_key, 
count(profit) as count, 
sum(price_USD_buy)/count(profit) as avg_buy_price,
sum(price_USD_sell)/count(profit) as avg_sell_price,
round(sum(profit),5) as profit_sum, 
round(avg(profit),5) as profit_avg, 
round(sum(profit)/sum(price_USD_buy),5) as percent_profit,
cast(gain_count as float)/count(profit) as percent_positive_trades,
cast(loss_count as float)/count(profit) as percent_negative_trades,
gain_avg,
gain_count,
loss_avg,
loss_count

FROM df_analysis_summary 

LEFT JOIN
    (
    SELECT
    analysis_key as id,
    count(profit) as gain_count,
    avg(profit) gain_avg
    FROM df_analysis_summary
    WHERE profit > 0  
    GROUP BY analysis_key
    ) as p on p.id = df_analysis_summary.analysis_key
    
LEFT JOIN
    (
    SELECT
    analysis_key as id,
    count(profit) as loss_count,
    avg(profit) as loss_avg
    FROM df_analysis_summary
    WHERE profit <= 0  
    GROUP BY analysis_key
    ) as p1 on p1.id = df_analysis_summary.analysis_key


    
GROUP BY 
analysis_key, 
gain_avg,
gain_count,
loss_avg,
loss_count 

ORDER BY percent_profit desc

"""

print(ps.sqldf(query, locals()))



#print(df_analysis_summary)

#df_analysis_summary.to_csv('summary.csv', index=False)