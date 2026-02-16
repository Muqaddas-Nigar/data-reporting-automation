

def validator(raw_df, clean_df):
    clean_index = clean_df.index
    raw_df.loc[clean_index, 'status'] = 'Cleaned'
    return raw_df