from prefect.infrastructure import DockerContainer

#alternative to creating dockerContainer block in UI
docker_block = DockerContainer(
    image="oluwagbotty/prefect:dataeng",
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

docker_block.save("dataeng", overwrite=True)