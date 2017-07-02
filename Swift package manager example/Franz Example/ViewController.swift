//
//  ViewController.swift
//  Franz Example
//
//  Created by Luke Lau on 02/07/2017.
//  Copyright © 2017 Luke Lau. All rights reserved.
//

import UIKit
import Franz

class ViewController: UIViewController, HighLevelConsumerDelegate {
	
	func consumerDidReturnMessage(_ message: Message, offset: Int64) {
		print(String(data: message.value, encoding: .utf8) ?? "Couldn't decode message")
	}
	
	func consumerIsReady(_ consumer: Consumer) {
		print("Consumer is ready")
		if let highLevelConsumer = consumer as? HighLevelConsumer {
			highLevelConsumer.poll()
		}
	}
	
	var cluster: Cluster!

	override func viewDidLoad() {
		super.viewDidLoad()
		// Do any additional setup after loading the view, typically from a nib.
		
		cluster = Cluster(
			brokers: [
				("localhost", 9092)
			],
			clientId: "replica-test"
		)
		
		let consumer = cluster.getHighLevelConsumer("replica", partition: 0, groupId: "replica-group", delegate: self)
		
		DispatchQueue.main.async {
			while true {
				self.cluster.batchMessage("replica", partition: 0, message: "1")
				self.cluster.batchMessage("replica", partition: 0, message: "2")
				self.cluster.batchMessage("replica", partition: 0, message: "3")
				
				do {
					try self.cluster.sendBatch("replica", partition: 0)
				} catch ClusterError.noBatchForTopicPartition(let topic, let partition) {
					print("Error: No batch for topic \(topic), \(partition)")
				} catch {
					print("Error")
				}
				sleep(1)
			}
		}
	}

	override func didReceiveMemoryWarning() {
		super.didReceiveMemoryWarning()
		// Dispose of any resources that can be recreated.
	}
	
	func topicPartitionLeaderNotFound(_ topic: String, partition: Int32) {
		print("Topic Partition Leader Not Found")
	}
	
	func shouldCommitOffset(_ topic: String, partition: Int32, offset: Int64) -> Bool {
		print("Should Commit Offset")
		return true
	}
	
	func shouldAttachOffsetMetadata(_ topic: String, partition: Int32, offset: Int64) -> String? {
		print("Should Attach Offset Metadata")
		return "mooser"
	}
	
	func offsetDidCommit(_ topic: String, partition: Int32, offset: Int64) {
		print("Offset Did Commit")
	}
	
	func offsetCommitDidFail(
		_ topic: String,
		partition: Int32,
		offset: Int64,
		errorId: Int16,
		errorDescription: String
		) {
		print("Offset Commit Did Fail")
	}
	
	func shouldRetryFailedOffsetCommit(
		_ topic: String,
		partition: Int32,
		offset: Int64,
		errorId: Int16,
		errorDescription: String
		) -> Bool {
		return false
	}

}

