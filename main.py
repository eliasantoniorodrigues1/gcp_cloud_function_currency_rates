import pandas as pd
from google.cloud import bigquery
from time import sleep
from datetime import datetime, timedelta
import os
import sys


sys.path.append('..')
# base directory
BASE_DIR = os.path.join(os.path.dirname(__file__))

# adding credentials in enviroment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
    BASE_DIR, 'serasa_google.json')

# data configuration
project_id = 'ga360-localiza'
table_name = 'crm_analytics.moedas_historico_cotacoes'

# ==============================================================================
# autentication
# auth.authenticate_user()

# get the general client created above below the importations
client = bigquery.Client()
table = client.get_table(table_name)

# dict compherension to define the destination scheme
generate_schema = [{'name': i.name, 'type': i.field_type}
                   for i in table.schema]

# ==============================================================================


def generate_list_dates() -> list:
    '''
      this function generates the list of dates by looking for the last
      updated date in our table "moedas_historico_cotacoes"
      it returns a list of dates to be iterate in the function 
      "get_currency_history"

    '''
    query = f'''SELECT max(data_atualizacao) as data FROM `{table}`'''
    query_job = client.query(query)

    results = query_job.result()

    # setting up start date
    delta = timedelta(days=1)

    # finding the gap of dates missing in our table
    start_date = [row.data for row in results][0]
    end_date = datetime.today().date() - delta
    days = end_date - start_date

    # iterating over the days to collect currency rates
    dates = []
    for _ in range(days.days + 1):
        dates.append(str(start_date))
        start_date = start_date + delta

    return dates


def get_currency_history(code: str, dates: list):
    '''
    this function use a website called "www.xe.com" to collect the cotation
    of your desireble currency rate.
    the function will iterate over a list of dates and then make a request
    on the base url.

    params: code: currency code like: USD
    params: dates: a list containing the desirable dates to collect the
    rates history.

    '''
    # pega dados dos ultimos 5 anos
    consolidado = []
    for date in dates:
        try:
            # url
            url = f'https://www.xe.com/currencytables/?from={code}&date={date}#table-section'

            # obtem os dados do site
            data = pd.read_html(url)
            df = data[0]
            df['data_atualizacao'] = [date for _ in range(len(df))]

            consolidado.append(df)
            sleep(.7)
        except Exception as e:
            print(e)
            continue
    return consolidado


def insert_gcp(df: pd.DataFrame, project_id: str, table: str, method='append'):
    '''
        this function find out the destination schema and perform an insert
        into the table passed as a parameter
        params: df : your dataset with record to be inserted into a table
        params: project_id: A string containing a name of your project
        params: table: A name using dataset.table notation to set a destination
        to your insert process.

    '''
    # rename to columns from my dataset according to my table
    df.columns = [i.name for i in table.schema]

    # inserting with generate Schema
    df.to_gbq(destination_table=table_name, project_id=project_id,
              table_schema=generate_schema, if_exists=method)


def remove_duplicates():
    query = f'''
    -- remove os duplicados da tabela
    CREATE OR REPLACE TABLE `ga360-localiza.crm_analytics.moedas_historico_cotacoes` AS
    SELECT 
        DISTINCT 
        * 
    FROM 
        `ga360-localiza.crm_analytics.moedas_historico_cotacoes`
    '''
    query_job = client.query(query)
    results = query_job.result()
    return results

def main(request):
    # calling the function to generate missing dates
    dates = generate_list_dates()

    # currencies list
    currencies = ['ARS', 'COP', 'USD']

    all_rates = []
    for currency in currencies:
        print(f'Coletando dado da moeda: {currency}')
        consolidado = get_currency_history(code=currency, dates=dates)

        # cria o dataset consolidado
        df = pd.concat(consolidado)

        # adiciona o codigo da moeda
        df['code'] = [currency for _ in range(len(df))]

        # reset index
        df = df.reset_index()

        # clean data
        new_columns = ['paridade', 'nome', 'unidades_por_codigo',
                       'codigo_por_unidade', 'data_atualizacao', 'codigo']

        # drop column index
        df.drop(columns=['index'], inplace=True)
        df.columns = new_columns
        columns = df.columns.tolist()

        # changing position of columns on dataset
        new_position_column = df.pop(columns[-1])
        df.insert(0, 'codigo', new_position_column)
        all_rates.append(df)

    # consolidate history
    df = pd.concat(all_rates)

    # save file locally
    # df.to_csv(f'currency_history_rates.csv', index=False)

    # inserting into GCP
    insert_gcp(df=df, project_id=project_id, table=table, method='append')

    # remove duplicates
    r = remove_duplicates()
    print(f'Duplicatas removidas {r}')

    return 'Processo finalizado com sucesso!'
