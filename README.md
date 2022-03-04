## Running the sample code

1. Start a local PostgresSQL server on default port 5432 and a Kafka broker on port 9092. The included `docker-compose.yml` starts everything required for running locally.

    ```shell
    docker-compose up -d

    # creates the tables needed for Akka Persistence
    # as well as the offset store table for Akka Projection
    docker exec -i class-booking-service-postgres-db-1 psql -U class-booking -t < ddl-scripts/create_tables.sql
    
    # creates the user defined projection table.
    docker exec -i class-booking-service-postgres-db-1 psql -U class-booking -t < ddl-scripts/create_user_tables.sql
    ```

2. Start a first node:

    ```shell
    sbt -Dconfig.resource=local1.conf run
    ```

3. (Optional) Start another node with different ports:

    ```shell
    sbt -Dconfig.resource=local2.conf run
    ```

4. (Optional) More can be started:

    ```shell
    sbt -Dconfig.resource=local3.conf run
    ```

5. Check for service readiness

    ```shell
    curl http://localhost:9101/ready
    ```

6. Try it with [grpcurl](https://github.com/fullstorydev/grpcurl):

    ```shell
    # add participant to class
    grpcurl -d '{"classId":"class1", "participant":{"name": "Elena"}}' -plaintext 127.0.0.1:8101 booking.ClassBookingService.AddParticipant
    
    # get class
    grpcurl -d '{"classId":"class1"}' -plaintext 127.0.0.1:8101 booking.ClassBookingService.Get
    
    # remove participant from class
    grpcurl -d '{"classId":"class1", "participant":{"name": "Elena3"}}' -plaintext 127.0.0.1:8101 booking.ClassBookingService.RemoveRequest
    
    # close class
    grpcurl -d '{"classId":"class1"}' -plaintext 127.0.0.1:8101 booking.ClassBookingService.CloseClass
    
    # WIP get percentage of spots filled
    grpcurl -d '{"classId":"class1"}' -plaintext 127.0.0.1:8101 booking.ClassBookingService.GetClassOccupancyPercentage
    ```

    or same `grpcurl` commands to port 8102 to reach node 2.
