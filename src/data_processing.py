import dask.dataframe as dd

def load_raw_meter_data(file_path):
    # Efficiently load millions of daily smart meter records
    df = dd.read_csv(file_path)
    return df

def clean_and_prepare(df):
    df['date'] = dd.to_datetime(df['date'])
    df = df.dropna(subset=['meter_id', 'date', 'consumption'])
    df = df.sort_values(['meter_id', 'date'])
    return df

def save_processed(df, out_path):
    df.compute().to_csv(out_path, index=False)

if __name__ == "__main__":
    df = load_raw_meter_data("data/raw/daily_meter_data.csv")
    df = clean_and_prepare(df)
    save_processed(df, "data/processed/daily_meter_data_clean.csv")