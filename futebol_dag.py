import pandas as pd

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

dfBrasileirao=pd.read_csv('/opt/airflow/dags/Brasileirao_Matches.csv')
dfPremierLeague=pd.read_csv('/opt/airflow/dags/PremierLeague_Matches.csv')

default_args = {
		'owner': 'airflow',
        'depends_on_past': False,
		'start_date': datetime(2023, 6, 23),
		'retries': 1,
		'retry_delay': timedelta(minutes=5)
}

generetor_csv_dag = DAG('dag',
		default_args=default_args,
		description='DAG for check Futebol Scores',
		schedule_interval='@daily', 
		catchup=False,
		tags=['dag, data']
)

def generate_new_csv():
		dfBrasileiraoFormatado = tratarBrasileirao()
		dfPremierLeagueFormatado = tratarPremierLeague()

		frames = [dfBrasileiraoFormatado, dfPremierLeagueFormatado]

		result = pd.concat(frames)
		result.sort_values('home_team', ascending=True, inplace=True)
		result.drop_duplicates(inplace=True)
		result.reset_index(drop=True, inplace=True)
		result.to_csv('/opt/airflow/dags/comparacao_brasil_inglaterra.csv')

def tratarBrasileirao():
       
       tratandoDF = dfBrasileirao.drop(columns=['home_team_state', 'away_team_state', 'round'])

       tratandoDF.rename(columns={"home_goal": "home_goals", "away_goal": "away_goals"})
       tratandoDF.dropna(subset = ['home_team', 'away_team', 'home_goals', 'away_goals'], inplace=True)
       tratandoDF.columns=[x.upper() for x in tratandoDF.columns]
       tratandoDF['season'] = tratandoDF['season'].astype(int)

       tratandoDF = ordernaColunas(tratandoDF)

       return tratandoDF

def tratarPremierLeague():
       
      tratandoDF = dfPremierLeague.drop(columns=['result'])
      tratandoDF.columns=[x.upper() for x in tratandoDF.columns]
       
      tratandoDF = ordernaColunas(tratandoDF)

      return tratandoDF

def ordernaColunas(dataFrame):
       dataFrame = dataFrame.reindex(columns=['home_team', 'away_team', 'home_goals', 'away_goals', 'season'])
       return dataFrame


start_task = DummyOperator(task_id='start_task', dag=generetor_futebol_dag)

csv_task = PythonOperator(task_id='csv_task', python_callable=generate_new_csv, dag=generetor_futebol_dag)


end_task = DummyOperator(task_id='end_task', dag=generetor_futebol_dag)

start_task >> csv_task >> end_task