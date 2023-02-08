from prefect import flow

from prefect_sqlalchemy import SqlAlchemyConnector

from datetime import timedelta
from prefect.tasks import task_input_hash

@task(retries=3, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))

connection_block = SqlAlchemyConnector.load("posstgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        #df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect block register -m prefect_sqlalchemy

prefect orion start

handling error "alembic.util.exc.CommandError: Can't locate revision identified by 'bb38729c471a'"
rm ~/.prefect/orion.db*

prefect deployment build ./parameterized_flow.py:parent_etl_flow -n "parameterized ETL"
prefect deployment apply parent_etl_flow-deployment.yaml
prefect agent start --work-queue "default"

{, "month":[1,2], "year":2021, "color":"yellow }

prefect block register -m prefect_gcp

#note build docker image first, then test it before uploading
#docker image run -it oluwagbotty/prefect:dataeng

docker image build -t oluwagbotty/prefect:dataeng .
docker login #to login into docker hub
docker image push oluwagbotty/prefect:dataeng


pip install prefect-docker

prefect block register -m prefect_docker

prefect profile ls
prefect agent start --work-queue "default"

prefect deployment run <name of the deployment> -p "months=[3,4]"
prefect deployment run parent-etl-flow/docker-flow -p "months=[3,4]"