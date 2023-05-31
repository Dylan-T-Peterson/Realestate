import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa

if __name__ == '__main__':
    #sources from Redfin, a national realestate brokerage
    #should use latest dataset (monthly)
    data = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz'
    df = pd.read_csv(filepath_or_buffer=data,
                     delimiter='\t', compression={'method':'gzip'})
    
    #filters out unwanted excess data by dropping chosen columns
    for filters in ['mom', 'yoy', 'region_type', 'period_end', 
                    'is_seasonally_adjusted', 'price_drops',
                    'off_market_in_two_weeks','property_type_id',
                    'months_of_supply','parent_metro_region_metro_code',
                    'table_id', ]:
        df.drop(
            labels=df.columns[df.columns.str.contains(filters)],
            inplace=True, axis=1)

    df.drop(labels='state', inplace=True, axis=1)
    
    #rearrange and format certain columns to make more human readable
    #and type assignments for further filtering
    df.rename(columns={'region' : 'zip_code'}, inplace=True)
    df.zip_code = df.zip_code.str.slice(start=-5)

    df.city = df.parent_metro_region.str.slice(stop=-4)
    df.drop(labels='parent_metro_region', inplace=True, axis=1)
    
    df.period_begin = pd.to_datetime(df.period_begin)
    df.zip_code = df.zip_code.astype('int32')

    #optional further data filtering, using these parameters for this project
    df = df[(df.median_list_price < 1_000_000) &
            (df.median_sale_price < 1_000_000) &
            (df.city == 'Anchorage') &
            (df.state_code == 'AK') &
            (df.period_begin.dt.year > (pd.Timestamp.now().year - 5))]

    #type assignment to minimize file size
    for i in ['city', 'state_code', 'property_type', ]:
        df[i] = df[i].astype('string')

    for i in ['period_begin', 'last_updated', ]:
        df[i] = df[i].astype('datetime64[ns]')
    
    for i in ['homes_sold', 'period_duration', ]:
        df[i] = df[i].astype('int16')
        
    for i in ['median_sale_price', 'median_list_price', ]:
        df[i] = df[i].astype('int32')

    for i in ['median_ppsf', 'median_list_ppsf', 'pending_sales',
              'new_listings', 'inventory', 'median_dom','avg_sale_to_list',
              'sold_above_list', ]:
        df[i] = df[i].astype('float32')
    
    #reordering by period date for chronological order
    df.sort_values(by=['period_begin', 'zip_code'], inplace=True)
    df.reset_index(inplace=True, drop=True)    
    
    pq.write_table(pa.Table.from_pandas(df),
                    'data/market_formatted.parquet.br',
                    compression='brotli', )