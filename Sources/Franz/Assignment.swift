//
//  Assignment.swift
//  Franz
//
//  Created by Luke Lau on 14/07/2017.
//

import Foundation

extension Cluster {
	
	func assignRoundRobin(members: [MemberId], partitions: [TopicName: [Partition]]) -> [MemberId: [TopicName: [PartitionId]]] {
		var partitionAndTopics = partitions.reduce([(TopicName, PartitionId)](), { (total, arg1) in
			let (topic, partitions) = arg1
			
			return total + partitions.map { (topic, $0.id) }
		})
		
		var result = [MemberId: [TopicName: [PartitionId]]]()
		
		for member in members {
			result[member] = [TopicName: [PartitionId]]()
			for topic in partitions.keys {
				result[member]![topic] = [PartitionId]()
			}
		}
		
		var i = 0
		while !partitionAndTopics.isEmpty {
			let currentMember = members[i % members.count]
			let (topic, partition) = partitionAndTopics.removeFirst()
			result[currentMember]![topic]!.append(partition)
			
			i += 1
		}
		
		return result
	}
	
	func assignRoundRobin(members: [MemberId], topics: [TopicName], completion: @escaping ([MemberId: [TopicName: [PartitionId]]]) -> Void) {
		getParitions(for: topics) { partition in
			completion(self.assignRoundRobin(members: members, partitions: partition))
		}
	}
}
