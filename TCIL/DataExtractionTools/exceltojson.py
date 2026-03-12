import pandas as pd
import json

# Provide the actual path to your Excel file
file_path = "/Users/atirathgaming/Downloads/simplemaps_worldcities_basicv1.77/worldcities.xlsx"

# Load the Excel data into a DataFrame
df = pd.read_excel(file_path)

# Print the first few rows to see the column names
print("Excel data columns: ", df.columns)
print(df.head())

# Check column names; replace 'Lon' with the actual column name (for longitude)
json_structure = {
    "coordinates": [
        {
            "cityName": row["city"],
            "geoLocation": {
                "lat": row["lat"],
                "lon": row["lng"]  # Assuming the column is actually named 'lng'
            }
        }
        for index, row in df.iterrows()
    ]
}

# Save JSON to a file with ensure_ascii=False
output_path = "/Users/atirathgaming/Documents/cities_coordinates1.json"
with open(output_path, 'w', encoding='utf-8') as json_file:
    json.dump(json_structure, json_file, ensure_ascii=False, indent=2)

print(f"JSON file created successfully at: {output_path}")
