# rabbitmq

Encapsulation to facilitate the use of rabbitMQ

## Example

![image](https://user-images.githubusercontent.com/1699113/58917742-f82c2000-86fd-11e9-80ef-e5e2c1178f28.png)

### Consumer

`$ ENV=local CLOUDAMQP_URL=amqp://user:password@domain.com/vhost go run example/consumer/main.go`

### Publisher

`$ ENV=local CLOUDAMQP_URL=amqp://user:password@domain.com/vhost go run example/publisher/main.go`
