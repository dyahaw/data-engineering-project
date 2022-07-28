import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import datetime
import sqlite3


def fetch_data(**kwargs):
    df_extract = pd.read_csv(
        '/usr/local/airflow/dags/Project/cardio_train.csv')
    df_extract[['id', 'age', 'gender', 'height', 'weight', 'ap_hi', 'ap_lo', 'cholesterol', 'gluc', 'smoke', 'alco', 'active', 'cardio']
               ] = df_extract['id;age;gender;height;weight;ap_hi;ap_lo;cholesterol;gluc;smoke;alco;active;cardio'].str.split(';', expand=True)
    df_extract = df_extract.drop(
        ['id;age;gender;height;weight;ap_hi;ap_lo;cholesterol;gluc;smoke;alco;active;cardio'], axis=1)
    df_extract.to_csv(
        "/usr/local/airflow/dags/Project/cardioExtract.csv")


def transform_data(**kwargs):
    df_transform = pd.read_csv(
        '/usr/local/airflow/dags/Project/cardioExtract.csv', index_col=[0])

    # Mengubah kolom Age yang memiliki satuan hari menjadi tahun dengan cara membagi /365
    def years():
        umur = df_transform['age'] / 365
        return umur

    # Mengubah tipe data menjadi int dan float
    df_transform['age'] = years()
    df_transform['id'] = df_transform['id'].astype(int)
    df_transform['age'] = df_transform['age'].astype(int)
    df_transform['gender'] = df_transform['gender'].astype(int)
    df_transform['height'] = df_transform['height'].astype(int)
    df_transform['weight'] = df_transform['weight'].astype(float)
    df_transform['ap_hi'] = df_transform['ap_hi'].astype(int)
    df_transform['ap_lo'] = df_transform['ap_lo'].astype(int)
    df_transform['cholesterol'] = df_transform['cholesterol'].astype(int)
    df_transform['gluc'] = df_transform['gluc'].astype(int)
    df_transform['smoke'] = df_transform['smoke'].astype(int)
    df_transform['alco'] = df_transform['alco'].astype(int)
    df_transform['active'] = df_transform['active'].astype(int)
    df_transform['cardio'] = df_transform['cardio'].astype(int)

    # Cleaning Data

    # 1. Menghilangkan bobot dan tinggi badan yang berada di bawah 2,5% atau di atas 97,5% dari kisaran tertentu.
    # Karena dapat diketahui bahwa tinggi minimum 55 cm, berat minimum adalah 10 kg serta usia minimum 29kg, berat
    # maksimum 250 cm dan berat maksmimum 200kg yang tidak relevan.
    df_transform.drop(df_transform[(df_transform['height'] > df_transform['height'].quantile(0.975)) | (
        df_transform['height'] < df_transform['height'].quantile(0.025))].index, inplace=True)
    df_transform.drop(df_transform[(df_transform['weight'] > df_transform['weight'].quantile(0.975)) | (
        df_transform['weight'] < df_transform['weight'].quantile(0.025))].index, inplace=True)

    # 2. Selain itu, dalam beberapa kasus tekanan diastolik lebih tinggi dari sistolik, yang juga salah.
    df_transform.drop(df_transform[(df_transform['ap_hi'] > df_transform['ap_hi'].quantile(0.975)) | (
        df_transform['ap_hi'] < df_transform['ap_hi'].quantile(0.025))].index, inplace=True)
    df_transform.drop(df_transform[(df_transform['ap_lo'] > df_transform['ap_lo'].quantile(0.975)) | (
        df_transform['ap_lo'] < df_transform['ap_lo'].quantile(0.025))].index, inplace=True)

    # calculate the BMI score
    df_transform['BMI'] = ((df_transform['weight']) /
                           (df_transform['height']/100)**2)

    # 3. Drop outliers in body mass index
    df_transform.drop(df_transform.query(
        'BMI >60 or BMI <15').index, axis=0, inplace=True)

    # categorize normal & abnormal
    def bmi_categorize(bmi_score):
        if 18.5 <= bmi_score <= 25:
            return "Normal"
        else:
            return "Abnormal"
    df_transform["BMI_State"] = df_transform["BMI"].apply(
        lambda x: bmi_categorize(x))

    # Hasil Transform Data sesudah di cleaning di Dump ke csv
    df_transform.to_csv("/usr/local/airflow/dags/Project/cardioTransform.csv")


def load_db(**kwargs):
    df_load = pd.read_csv(
        '/usr/local/airflow/dags/Project/cardioTransform.csv', index_col=[0])
    path_db = "/usr/local/airflow/dags/Project/ProjectAkhirDE.db"
    conn = sqlite3.connect(path_db)
    cur = conn.cursor()
    cur.execute('''
                    CREATE TABLE IF NOT EXISTS Cardio(
                        id INT PRIMARY KEY NOT NULL,
                        age INT NOT NULL,
                        gender INT NOT NULL,
                        height INT NOT NULL,
                        weight FLOAT NOT NULL,
                        ap_hi INT NOT NULL,
                        ap_lo INT NOT NULL,
                        cholesterol INT NOT NULL,
                        gluc INT NOT NULL,
                        smoke INT NOT NULL,
                        alco INT NOT NULL,
                        active INT NOT NULL,
                        cardio INT NOT NULL,
                        BMI FLOAT NOT NULL,
                        BMI_State TEXT NOT NULL
                    );
                ''')
    # memasukan data ke database
    df_load.to_sql("Cardio", conn, if_exists='replace', index=False)
    conn.commit()


with DAG(
    dag_id="Project_AkhirDE",
    start_date=datetime.datetime(2021, 12, 7),
    schedule_interval="@daily",
    catchup=False
) as dag:

    fetch_dataCardio = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
    )

    transform_dataCardio = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_db,
    )

fetch_dataCardio >> transform_dataCardio >> load_data
