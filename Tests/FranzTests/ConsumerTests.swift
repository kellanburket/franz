//
//  ConsumerTests.swift
//  FranzTests
//
//  Created by Luke Lau on 01/08/2017.
//

import XCTest
@testable import Franz

class ConsumerTests: XCTestCase {
	
	var cluster: Cluster!
	
	class TestBroker: Broker {
		
		override func getGroupCoordinator(groupId: String, clientId: String, callback: @escaping (GroupCoordinatorResponse) -> Void) {
			callback(GroupCoordinatorResponse(errorCode: 0, coordinatorId: 0, coordinatorHost: "192.0.0.1", coordinatorPort: 0))
		}

		override func join(groupId: String, subscription: [TopicName], clientId: String, callback: ((GroupMembership) -> ())?) {
			let group = Group(broker: self, clientId: clientId, groupProtocol: .consumer, groupId: "newgroup", generationId: 0)

			group.topics = Set(subscription)

			let member = Member(name: "test", metadata: "")

			let membership = GroupMembership(group: group, memberId: "test", members: [member])
			
			groupMembership[groupId] = membership
			
			callback?(membership)
		}
		
		override func describeGroups(_ groupId: String, clientId: String, callback: ((String, GroupState) -> ())?) {
			callback?(groupId, .AwaitingSync)
		}
		
		override func fetchOffsets(groupId: String, topics: [TopicName : [PartitionId]], clientId: String, callback: @escaping ([TopicName : [PartitionId : Offset]]) -> ()) {
			callback(topics.mapValues { _ in [0: 0]})
		}
		
		override func syncGroup(_ groupId: String, generationId: Int32, memberId: String, topics: [TopicName : [PartitionId]], userData: Data, clientId: String, version: ApiVersion, callback: ((GroupMemberAssignment) -> ())?) {
			callback?(GroupMemberAssignment(topics: topics, userData: Data(), version: .defaultVersion))
		}
		
		class FakeConnection: Connection {
			
			required init(ipv4: String, port: Int32, broker: Broker, clientId: String) {
				
			}
			
			func generateFetchResponse(messages: [MessageSetItem], offset: Offset) -> Data {
				let messageSet = MessageSet(values: messages)
				
				//partition id
				var partitionData = PartitionId(0).data
				//error code
				partitionData += Int16(0).data
				//highwater mark offset
				partitionData += offset.data
				//message set size
				partitionData += messages.reduce(0) { $0 + Int32($1.dataLength) }.data
				//message set
				partitionData += messageSet.data
				
				let partitionResponse = PartitionedFetchResponse(data: &partitionData)
				
				var topicData = "test".data
				topicData += KafkaArray<PartitionedFetchResponse>([partitionResponse]).data
				
				let topicalFetchResponse = TopicalFetchResponse(data: &topicData)
				
				let responseData = KafkaArray<TopicalFetchResponse>([topicalFetchResponse]).data
				return responseData
			}
			
			func write(_ request: KafkaRequest, callback: RequestCallback?) {
				
				if let request = request as? TopicMetadataRequest, let message = request.message as? TopicMetadataRequestMessage {
					let brokers = [Broker(ipv4: "192.0.0.1", port: 0)]
					
					let partition = Partition(partitionErrorCode: 0, partitionId: 0, leader: 0, replicas: [], isr: [])
					
					let topics = message.values.values.map { KafkaTopic(errorCode:0, name: $0, partitionMetadata: [partition]) }
					
					var data = KafkaArray<Broker>(brokers).data
					data += KafkaArray<KafkaTopic>(topics).data
					
					callback?(data)
				}
				
				if let request = request as? FetchRequest, let message = request.message as? FetchRequestMessage {
					guard let topic = message.topics.first(where: { $0.topicName == "test" }), let partition = topic.partitions.first else {
						return
					}
					
					if partition.offset < 2 {
						callback?(generateFetchResponse(messages: [MessageSetItem(value: "hello"), MessageSetItem(value: "world")], offset: 2))
					}
					if partition.offset == 2 {
						callback?(generateFetchResponse(messages: [MessageSetItem(value: "further")], offset: 3))
					}
				}
			}
		}
		
		override func connect(_ clientId: String) -> Connection {
			return FakeConnection(ipv4: "192.0.0.1", port: 0, broker: self, clientId: "test")
		}
	}
	
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
		
		cluster = Cluster(brokers: [], clientId: "test")
		cluster.brokers = [TestBroker(ipv4: "192.0.0.1", port: 0)]
    }
    
    func testReceiveMessages() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
		let consumer = cluster.getConsumer(topics: ["test"], groupId: "newgroup")
		
		let helloExpectation = expectation(description: "Receive the message hello"),
			worldExpectation = expectation(description: "Receive the message world"),
			furtherExpectation = expectation(description: "Receive the message futher")
		
		consumer.listen { message in
			let string = String(data: message.value, encoding: .utf8)
			if string == "hello" {
				helloExpectation.fulfill()
			}
			if string == "world" {
				worldExpectation.fulfill()
			}
			if string == "further" {
				furtherExpectation.fulfill()
			}
		}
		
		waitForExpectations(timeout: 1)
    }
	
	func testDoesntReceiveUnsubscribedTopics() {
		
		let consumer = cluster.getConsumer(topics: ["foo"], groupId: "newgroup")
		
		consumer.listen { _ in
			XCTFail("Shouldn't have received a message")
		}
		
		Thread.sleep(forTimeInterval: 1)
	}
    
}
