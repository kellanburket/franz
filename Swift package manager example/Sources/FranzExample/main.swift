import Foundation
import Franz

let cluster = Cluster(brokers: [("localhost", 9092)], clientId: "FranzExample")

let consumer = cluster.getConsumer(topics: ["test"], groupId: "group")
consumer.listen { message in
	print(String(data: message.value, encoding: .utf8)!)
}

cluster.sendMessage("test", message: "Hello world!")
