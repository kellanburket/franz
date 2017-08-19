//
//  Consumer.swift
//  Pods
//
//  Created by Kellan Cummings on 2/5/16.
//
//

import Foundation

public class Consumer {
	private let cluster: Cluster
	internal var broker: Broker?
	internal var membership: GroupMembership?
	internal let joinedGroupSemaphore = DispatchSemaphore(value: 0)
	
	internal init(cluster: Cluster, groupId: String) {
		self.cluster = cluster
		
		if #available(OSX 10.12, *) {
			Timer.scheduledTimer(withTimeInterval: 5, repeats: true) { _ in self.commitGroupoffsets() }
		} else {
			// Fallback on earlier versions
			Timer.scheduledTimer(timeInterval: 5, target: self, selector: #selector(commitGroupoffsets), userInfo: nil, repeats: true)
		}
	}
	
	func subscribe(topic: String) {
		topicsToSubscribeTo.append(topic)
		DispatchQueue(label: "FranzConsumerSubscribeQueue").async {
			self.joinedGroupSemaphore.wait()
			self.subscripeToTopics()
		}
	}
	
	private var topicsToSubscribeTo = [TopicName]()
	private func subscripeToTopics() {
		topicsToSubscribeTo.forEach { membership!.group.topics.insert($0) }
		cluster.addTargetTopics(topics: topicsToSubscribeTo)
		topicsToSubscribeTo.removeAll()
	}
	
	let listenQueue = DispatchQueue(label: "FranzConsumerListenQueue")
	
	var offsetsToCommit = [TopicName: [PartitionId: (Offset, OffsetMetadata?)]]()
	@objc private func commitGroupoffsets() {
		guard let groupId = self.membership?.group.id, let broker = self.broker else { return }
		broker.commitGroupOffset(groupId: groupId, topics: offsetsToCommit, clientId: cluster.clientId)
	}
	
	func listen(handler: @escaping (Message) -> Void) {
		listenQueue.async {
			self.joinedGroupSemaphore.wait()
			guard let membership = self.membership, let broker = self.broker else {
				return
			}
			
			self.subscripeToTopics()
			
			self.cluster.getParitions(for: Array(membership.group.topics)) { partitions in
				let ids = partitions.reduce([TopicName: [PartitionId]](), { (result, arg1) in
					let (key, value) = arg1
					var copy = result
					copy[key] = value.map { $0.id }
					return copy
				})
				
				broker.poll(topics: ids, groupId: membership.group.id, clientId: "test", replicaId: ReplicaId.none, callback: { topic, partitionId, offset, messages in
						messages.forEach(handler)
						
						if var topicOffsets = self.offsetsToCommit[topic] {
							topicOffsets[partitionId] = (offset, nil)
						} else {
							self.offsetsToCommit[topic] = [partitionId: (offset, nil)]
						}
				}, errorCallback: { error in
//					print(error.description)
				})
			}
		}
	}
}
