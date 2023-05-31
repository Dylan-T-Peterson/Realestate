import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


if __name__ == '__main__':
    data = 'data/market_formatted.parquet.br'
    df = pd.read_parquet(data)
    df1 = df[df.property_type != 'All Residential']
    sns.lineplot(data=df1, x=df1.period_begin, y=df1.median_sale_price,
    hue=df1.property_type, style=df1.property_type, )
    plt.show()