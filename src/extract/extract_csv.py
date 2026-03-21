import pandas as pd
import os
from src.utils.logger import get_logger

logger = get_logger("extract_csv")

def extract_csv():

    try:
        logger.info("Lecture du fichier CSV source")

        input_path = "data/input/data.csv"

        df = pd.read_csv(input_path)

        os.makedirs("data/raw", exist_ok=True)

        output_path = "data/raw/data.csv"

        df.to_csv(output_path, index=False)

        logger.info(f"Données CSV copiées vers {output_path}")

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction CSV : {e}")


if __name__ == "__main__":
    extract_csv()