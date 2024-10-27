import pandas as pd

df = pd.read_csv('postcodes.csv')

original_rows = len(df)

# Assuming the columns are named 'postcode' and 'brma_name'
df['prefix'] = df['postcode'].str[:2]  # Take the first 2 characters as the initial prefix

# Function to check if all postcodes with a given prefix map to the same brma_name
def check_prefix(group):
    if len(group['brma_name'].unique()) == 1:
        return pd.Series({'postcode': group['prefix'].iloc[0], 'brma_name': group['brma_name'].iloc[0]})
    return pd.Series({'postcode': None, 'brma_name': None})

# Start with 2-character prefix and increase specificity if needed
new_df = pd.DataFrame(columns=['postcode', 'brma_name'])

for prefix_length in range(2, 9): # assumes postcodes are not longer than 9 characters including the space
    # Group by current prefix and apply the check function
    result = df.groupby(df['postcode'].str[:prefix_length]).apply(check_prefix)
    result = result.dropna().reset_index(drop=True)  
    
    # Add the new prefix-based rows to the result
    new_df = pd.concat([new_df, result], ignore_index=True)
    
    # Remove rows that were successfully grouped
    df = df[~df['postcode'].str[:prefix_length].isin(result['postcode'])]
    
    # Break if all postcodes have been processed
    if df.empty:
        break
    
    # Update the prefix for the remaining postcodes
    df['prefix'] = df['postcode'].str[:prefix_length + 1]

# Add any remaining individual postcodes to the new dataframe
if not df.empty:
    new_df = pd.concat([new_df, df[['postcode', 'brma_name']]], ignore_index=True)

# Sort the dataframe by postcode and save to CSV
new_df = new_df.sort_values('postcode').reset_index(drop=True)
new_df.to_csv('postcodes2.csv', index=False)

print(f"Original number of rows: {original_rows}")
print(f"New number of rows: {len(new_df)}")
print(f"Reduction: {(1 - len(new_df) / original_rows) * 100:.2f}%")
