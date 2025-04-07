import pandas as pd
from pandasgui import show

# Load the cleaned CSV file
df = pd.read_csv("nyc_data_project/output/DOHMH_New_York_City_Restaurant_Inspection_T.csv")


show(df)




from src.sql_uploader import upload_to_sqlserver
import pandas as pd

df = pd.read_csv("nyc_data_project/output/DOHMH_New_York_City_Restaurant_Inspection_T.csv")
upload_to_sqlserver(df, table_name="Inspected_RestaurantData", database="Restaurants_Inspection")
