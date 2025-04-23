import os
from kaggle.api.kaggle_api_extended import KaggleApi

def download_datasets():
    
    # --------------  Kaggle credentials from environment variables ------------------------------
    kaggle_username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")

    if not kaggle_username or not kaggle_key:
        raise ValueError("Kaggle credentials are missing. Set KAGGLE_USERNAME and KAGGLE_KEY as environment variables.")

    # ---------------------Define DataSet --------------------------------------------
    datasets = {
        'User_Fitness_Activity_Data': ('fajobgiua/fitness-tracker-data', 'User_Fitness_Activity_Data'),
        'Heart_Data': ('winson13/heart-disease-dataset', 'Heart_Data'),
        'Cardiovascular_Data': ('mamta1999/cardiovascular-risk-data', 'Cardiovascular_Data'),
        'Sleep_Data': ('uom190346a/sleep-health-and-lifestyle-dataset', 'Sleep_Data'),
        'Daily_Food_Nutrition_Dataset': ('adilshamim8/daily-food-and-nutrition-dataset', 'Daily_Food_Nutrition_Dataset'),
    }

    # -------------------------ROW Data Path ------------------------------------------------------
    rowdata_path = "/app/data/ROW_DATA"

    # ------------------------Create Directory & Give Correct Permissions------------------------------
    os.makedirs(rowdata_path, exist_ok=True)
    os.chmod(rowdata_path, 0o755)

    # --------------------------Initialize and authenticate Kaggle API --------------------------------
    try:
        api = KaggleApi()
        api.authenticate()
    except Exception as e:
        raise RuntimeError(f"Failed to authenticate with Kaggle API: {e}")

    # ----------------------------Download DataSet Using For Loop -------------------------------------
    for dataset_name, (dataset_id, folder_name) in datasets.items():
        dataset_path = os.path.join(rowdata_path, folder_name)
        os.makedirs(dataset_path, exist_ok=True)

        print(f"Downloading {dataset_name}...")
        api.dataset_download_files(dataset_id, path=dataset_path, unzip=True)

        # Verify successful download
        files = os.listdir(dataset_path)
        if not files:
            print(f"Warning: No files found in {dataset_path}. Check dataset ID.")
        else:
            print(f"{dataset_name} saved in '{dataset_path}'")

    print("All datasets downloaded successfully!")

if __name__ == "__main__":
    download_datasets()
