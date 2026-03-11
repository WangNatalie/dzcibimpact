import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import os
load_dotenv()   

SUPABASE_URL = os.getenv("SUPABASE_URL")

supabase = create_client(SUPABASE_URL)

df = pd.read_csv("land_change.csv")
df = df.rename(columns={"project_id": "id", "land_change_wetland": "land_change_wetlands"})

for _, r in df.iterrows():
    supabase.table("dzcib_projects_solris").update({
        "land_change_forest_acres": r["land_change_forest"],
        "land_change_wetlands_acres": r["land_change_wetlands"],
        "land_change_tallgrass_prairie_acres": r["land_change_tallgrass_prairie"]
    }).eq("id", r["id"]).execute()