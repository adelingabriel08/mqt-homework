# mqt-homework

## To run this project, the following are required:
- .NET 8 SDK installed
- Docker running the kafka with a few services (zookeeper, schema-registry, kafka-rest at least), other services are optional
- make sure you update the urls for the above services in the code

## Running the project
- To run the project, open a terminal in the API project and run `dotnet watch run`
- A browser should open with a swagger page
- If the browser is not opening, check `https://localhost:7214` or `http://localhost:5248`