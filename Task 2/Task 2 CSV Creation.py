import os
import pandas as pd
from tqdm import tqdm

# Folder containing Excel files
folder_path = "C:/Users/ayan1/PycharmProjects/Webscraping/Task 2/Excel Data"

# Initialize an empty DataFrame to store the combined data
combined_df = pd.DataFrame(
    columns=['State Name', 'District Name', 'Hospital Name', 'Patient Name', 'Discharge Date', 'Amount'])

# List of Excel files in the folder
files = os.listdir(folder_path)#[:50]  # Limit to first 10 files

# Process each Excel file in the folder with a progress bar
for file_name in tqdm(files, desc="Processing files", unit="file"):
    file_path = os.path.join(folder_path, file_name)

    # Read the Excel file without assuming a header
    df = pd.read_excel(file_path, header=None, engine='openpyxl')

    # Extract header information from the first cell
    first_cell_data = df.iloc[0, 0]
    state_name = first_cell_data.split("State Name : ")[1].split(" , District Name : ")[0].strip()
    district_name = first_cell_data.split("District Name : ")[1].split(" , Hospital Name : ")[0].strip()
    hospital_name = first_cell_data.split("Hospital Name : ")[1].strip()

    # Remove the first two rows from the DataFrame and reset index
    df = df.iloc[2:].reset_index(drop=True)

    # Check if the DataFrame is not empty after removing the first two rows
    if not df.empty:
        # Insert state, district, hospital names horizontally for each remaining row
        df.insert(0, 'State Name', state_name)
        df.insert(1, 'District Name', district_name)
        df.insert(2, 'Hospital Name', hospital_name)

        # Remove the fourth column from the final DataFrame if it exists
        if df.shape[1] > 3:
            df = df.drop(df.columns[3], axis=1)

        # Add column names for remaining columns
        df.columns = ['State Name', 'District Name', 'Hospital Name', 'Patient Name', 'Discharge Date', 'Amount']

        # Append to the combined DataFrame
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    else:
        # If the cleaned file is empty, create a row with state, district, and hospital names filled with "NA"
        empty_df = pd.DataFrame([[state_name, district_name, hospital_name, "NA", "NA", "NA"]],
                                columns=['State Name', 'District Name', 'Hospital Name',
                                         'Patient Name', 'Discharge Date', 'Amount'])

        # Append the empty DataFrame to the combined DataFrame
        combined_df = pd.concat([combined_df, empty_df], ignore_index=True)

# Save the final combined DataFrame as a CSV file
combined_file = "final_combined_file.csv"
combined_df.to_csv(combined_file, index=False, header=True)  # Include header in the saved file
print(f"Final combined CSV saved as {combined_file}")