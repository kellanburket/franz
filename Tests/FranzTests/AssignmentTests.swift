//
//  AssignmentTests.swift
//  FranzTests
//
//  Created by Luke Lau on 23/07/2017.
//

import XCTest
@testable import Franz

class AssignmentTests: XCTestCase {
	
	func getMockPartition(id: Int) -> Partition {
		return Partition(partitionErrorCode: 0, partitionId: id, leader: 0, replicas: [Int](), isr: [Int]())
	}
	
    func testRoundRobin() {
		
		let topics = ["foo", "bar", "baz", "daz", "waz", "woz"]
		let partitions = topics.reduce([TopicName: [Partition]]()) { acc, next in
			var newAcc = acc
			newAcc[next] = [0, 1, 2, 3, 4, 5].map(getMockPartition)
			return newAcc
		}
		
		let cluster = Cluster(brokers: [(String, Int32)](), clientId: "test")
		
		let members = ["gary", "bary", "dary"]
		
		let result = cluster.assignRoundRobin(members: members, partitions: partitions)
		
		XCTAssertEqual(result["gary"]!["foo"]!, [0, 3])
		
		let combinations = result.flatMap { member, topics in
			topics.flatMap { topic, partitions in
				partitions.map { partition in
					(member, topic, partition)
				}
			}
		}
		
		var seen = [(MemberId, TopicName, PartitionId)]()
		for combination in combinations {
			if seen.contains(where: { memberId, topicName, partitionId in
				(memberId, topicName, partitionId) == combination
			}) {
				XCTFail("A partition was given out twice")
			}
			seen.append(combination)
		}
    }
    
}
