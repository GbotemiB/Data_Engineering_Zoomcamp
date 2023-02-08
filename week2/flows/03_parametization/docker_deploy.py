from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterized_flow import parent_etl_flow

docker_container_block = DockerContainer.load("dataeng-docker")

docker_dep = Deployment.build_from_flow(
    flow=parent_etl_flow,
    name="docker-flow",
    infrastructure=docker_container_block
)

if __name__=="__main__":
    docker_dep.apply()