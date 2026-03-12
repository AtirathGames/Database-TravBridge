import pandas as pd

def excel_to_json(file_path, output_file):
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Convert the DataFrame to JSON
    json_data = df.to_json(orient='records')

    # Save the JSON data to a file
    with open(output_file, 'w') as file:
        file.write(json_data)

    print(f"Data from {file_path} has been successfully converted to JSON and saved as {output_file}.")

# Specify the path to your Excel file and the output JSON file name
excel_path = 'data.xlsx'
json_output = 'data.json'

# Call the function
excel_to_json(excel_path, json_output)
