import pandas as pd

# Read the CSV file
df = pd.read_csv('postcodes.csv')

# Store the original number of rows
original_rows = len(df)

# Assuming the columns are named 'postcode' and 'borough'
df['prefix'] = df['postcode'].str[:2]  # Take the first 2 characters as the initial prefix

# Function to check if all postcodes with a given prefix map to the same borough
def check_prefix(group):
    if len(group['borough'].unique()) == 1:
        return pd.Series({'postcode': group['prefix'].iloc[0], 'borough': group['borough'].iloc[0]})
    return pd.Series()  # Return an empty Series instead of None

# Start with 2-character prefix and increase specificity if needed
new_df = pd.DataFrame(columns=['postcode', 'borough'])

for prefix_length in range(2, 9):
    # Group by current prefix and apply the check function
    result = df.groupby(df['postcode'].str[:prefix_length]).apply(check_prefix)
    result = result.dropna().reset_index(drop=True)  # Remove any empty results
    
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
    new_df = pd.concat([new_df, df[['postcode', 'borough']]], ignore_index=True)

# Sort the dataframe by postcode
new_df = new_df.sort_values('postcode').reset_index(drop=True)

# Save the new dataframe to a CSV file
new_df.to_csv('postcodes2.csv', index=False)

print(f"Original number of rows: {original_rows}")
print(f"New number of rows: {len(new_df)}")
print(f"Reduction: {(1 - len(new_df) / original_rows) * 100:.2f}%")
