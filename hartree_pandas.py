import pandas as pd
def cube_agg(df1, df2):
    """
    Takes two dataframes from example and creates new dataframe with new columns corresponding to requirements
    :param df1: dataframe from dataset1.csv
    :param df2: dataframe from dataset2.csv
    :return: result dataset with new columns for max_rating_by_party, total_status, total_legal_entity, total_counter_party, total_tier
    """
    df3 = pd.merge(df1, df2, on='counter_party')
    max_rating_by_party_col = df3.groupby('counter_party')['rating'].max()
    max_rating_by_party_col.name = 'max_rating_by_party' # probably not cleanest way to rename a col for a series
    df4 = pd.merge(df3, max_rating_by_party_col, on='counter_party', how='inner')

    cols = ['status', 'legal_entity', 'counter_party', 'tier']
    for col in cols:
        sum_col = df4.groupby(col)['value'].sum()
        sum_col.name = 'total_{}'.format(col)
        df4 = pd.merge(df4, sum_col, on=sum_col.index.name, how='inner')

    return df4


def run(argv=None):
    """Main entry point"""
    path = './data/hartree/dataset1.csv'
    path2 = './data/hartree/dataset2.csv'

    df1 = pd.read_csv(path)
    df2 = pd.read_csv(path2)

    result_df = cube_agg(df1, df2)
    result_df

    output_path = './data/hartree/pandas_result.csv'
    result_df.to_csv(output_path)

if __name__ == '__main__':
    run()