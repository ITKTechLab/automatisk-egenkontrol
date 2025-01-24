import psycopg2
import pandas as pd
from datetime import date, datetime, timedelta
import locale, os, logging
from dotenv import load_dotenv

locale.setlocale(locale.LC_ALL, 'da_DK.UTF-8')
load_dotenv(override=True)

# Set this flag to True to enable logging
ENABLE_LOGGING = True

# Logging level options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = logging.DEBUG  # Change this to set the desired logging level, WARNING is recommended for live environments.

# Setup logging - only if ENABLE_LOGGING is True
if ENABLE_LOGGING:
    LOG_FILE = os.path.join(os.getcwd(),"temp_logger.log")
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

try:
    connection = psycopg2.connect(
        user = os.getenv('database_user'), 
        password = os.getenv('database_password'), 
        host = os.getenv('host'), 
        port = os.getenv('port'), 
        database = os.getenv('database')
    )
    
    cursor = connection.cursor()
    date_from = datetime(2024, 9, 1)
    
    # SQL-query
    query = """
        WITH current_week AS (
            SELECT 
                EXTRACT(WEEK FROM CURRENT_TIMESTAMP) AS current_week,
                EXTRACT(DOW FROM CURRENT_TIMESTAMP) AS current_dow,
                EXTRACT(HOUR FROM CURRENT_TIMESTAMP) AS current_hour
        ),
        adjusted_data AS (
            SELECT 
                department, 
                FLOOR(EXTRACT(WEEK FROM time_index))::int AS uge_nr,
                EXTRACT(YEAR FROM time_index)::int AS raw_year,
                name,
                floor,
                appliance,
                room, 
                temperature,
                CASE 
                    WHEN FLOOR(EXTRACT(WEEK FROM time_index)) = 1 AND EXTRACT(MONTH FROM time_index) = 12 THEN EXTRACT(YEAR FROM time_index) + 1
                    WHEN FLOOR(EXTRACT(WEEK FROM time_index)) >= 52 AND EXTRACT(MONTH FROM time_index) = 1 THEN EXTRACT(YEAR FROM time_index) - 1
                    ELSE EXTRACT(YEAR FROM time_index)
                END AS year_adjusted
            FROM mttutorial."etrefrigerator-sensor"
            WHERE time_index >=  %s  -- Startdato (1. september 2024)
            AND time_index <= CURRENT_TIMESTAMP -- Til og med dags dato
            AND department IS NOT NULL
            AND floor IS NOT NULL
        )
        SELECT 
            department, 
            uge_nr,
            year_adjusted AS year,
            name,
            floor,
            appliance,
            room, 
            ROUND(AVG(temperature)::numeric, 1) AS gennemsnitstemperatur
        FROM adjusted_data
        GROUP BY department, uge_nr, year_adjusted, appliance, room, name, floor
        ORDER BY department, uge_nr, year, room, name;
    """
    cursor.execute(query, (date_from,))
    records = cursor.fetchall()
    
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(records, columns=colnames)
    
    df['department'] = df['department'].str.replace('Jesper ', 'Jespers ', regex=False)
    df['year'] = df['year'].astype(int)

    df = df[df['floor'] != 'Køkken']

    df['room_appliance'] = df.apply(
        lambda row: f"Etage: {row['floor']}\nRum: {row['room']}\nEnhed: {row['appliance']}" 
        if pd.notnull(row['floor']) and pd.notnull(row['room']) and pd.notnull(row['appliance'])
        else "", axis=1)

except (Exception, psycopg2.Error) as error:
    logging.error("Error connecting to PostgreSQL: %s", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        logging.info("PostgreSQL connection is closed")

grouped = df.groupby('department')

seneste_opdatering = datetime.now().strftime('%d-%m-%Y\nkl.: %H:%M:%S')

department_dfs = {}
for name, group in grouped:
    pivot_df = group.pivot_table(
        index=['uge_nr', 'year'], 
        columns='room_appliance', 
        values='gennemsnitstemperatur'
    ).reset_index()
    
    pivot_df.columns.name = None
    
    pivot_df.columns = [
        f"{col}" if isinstance(col, str) else f"{col[0]} {col[1]}"
        for col in pivot_df.columns
    ]
    
    department_dfs[name] = pivot_df


current_week = datetime.now().isocalendar()
print(f"Current Week: {current_week}")

current_year = datetime.now().year

for department, df in department_dfs.items():
    try:
        html_content = f"""
        <!DOCTYPE html>
        <html lang="da">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{department} - Temperaturmålinger</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC" crossorigin="anonymous"> 
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ text-align: center; margin-bottom: 20px; font-size: 30px; font-weight: bold; }}
                h2 {{ text-align: center; margin-bottom: 20px; font-size: 15px; }}
                .table-container {{ margin-bottom: 40px; }}
                .year-separator {{ font-size: 20px; font-weight: bold; margin: 30px 0; text-align: left; }}
                .table {{ margin: auto; width: auto; table-layout: auto; border: 1px solid #ddd; border-collapse: collapse; }}
                th, td {{ text-align: center; vertical-align: middle; border: 1px solid #ddd; }}
                th {{ white-space: pre-line;}}
                .table-hover tbody tr:hover {{ background-color: #f5f5f5; }}
            </style>
        </head>
        <body>
            <h1>Ugentlig temperaturmåling for {department}</h1>
            <h2>Seneste opdatering: {seneste_opdatering}</h2>
        """

        for year in sorted(df['year'].unique(), reverse=True):
            df_year = df[df['year'] == year].drop_duplicates()  
            df_year = df_year.sort_values(by=['uge_nr'], ascending=False)  
            df_year = df_year.copy()  
            for col in df_year.columns:
                if col.lower() not in ["uge_nr", "year"]:  
                    df_year[col] = df_year[col].apply(
                        lambda x: "Ingen Data" if pd.isna(x) 
                        else f"{x:.1f} °C".replace('.', ',') if isinstance(x, (int, float)) 
                        else x
                    )

            df_year.rename(columns=lambda col: (
                "Uge\nNummer" if col == "uge_nr" else
                "År" if col == "year" else
                col.replace("Etage: ", "")
                .replace("Rum: ", "")
                .replace("Enhed: ", "")
                .replace(": ", ":\n")
            ), inplace=True)

            html_content += f"""
            <div class="year-separator" style="text-align: center; font-weight: bold; margin-top: 20px; margin-bottom: 20px;">
                {department} - {year}
            </div>
            """

            html_table = df_year.to_html(
                index=False,
                classes="table table-hover table-striped table-bordered table table-sm",
                justify="center",
                escape=False 
            )

            html_content += f"""
            <div class="table-container">
                {html_table}
            </div>
            """

        html_content += """
        </body>
        </html>
        """
        file_name = os.path.join(os.getcwd(), "temperature_logger", "out_put", f"{department}.html")
        with open(file_name, "w", encoding="utf-8") as file:
            file.write(html_content)

        logging.info(f"The HTML file for department {department} has been updated and replaced with new data.")

    except Exception as e:
        
        logging.warning(f"There was an issue saving the HTML file for department {department}: {e}")