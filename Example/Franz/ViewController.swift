//
//  ViewController.swift
//  Franz
//
//  Created by kellanburket on 01/24/2016.
//  Copyright (c) 2016 kellanburket. All rights reserved.
//

import UIKit
import Franz

class ViewController: UIViewController, HighLevelConsumerDelegate {

    var cluster: Cluster!
    
    override func viewDidLoad() {
        super.viewDidLoad()
        
        cluster = Cluster(
            brokers: [
                ("127.0.0.1", 9092)
            ],
            clientId: "replica-test"
        )
        
        cluster.getHighLevelConsumer(
            "replica",
            partition: 0,
            groupId: "replica-group",
            delegate: self
        )

        dispatch_async(dispatch_get_global_queue(QOS_CLASS_BACKGROUND, 0)) {
            while true {
                self.cluster.batchMessage("replica", partition: 0, message: "1")
                self.cluster.batchMessage("replica", partition: 0, message: "2")
                self.cluster.batchMessage("replica", partition: 0, message: "3")

                do {
                    try self.cluster.sendBatch("replica", partition: 0)
                } catch ClusterError.NoBatchForTopicPartition(let topic, let partition) {
                    print("Error: No Batch for Topic (\(topic)), (\(partition))")
                    break
                } catch {
                    print("Error.")
                    break
                }
                sleep(1)
            }
        }
    }
        
    func consumerDidReturnMessage(message: Message, offset: Int64) {
        print(NSString(data: message.value, encoding: NSUTF8StringEncoding))
    }
    
    func shouldRetryFailedFetch(
        topic: String,
        partition: Int32,
        offset: Int64,
        errorId: Int16,
        errorDescription: String
    ) -> Bool {
        return true
    }
    
    func consumerIsReady(consumer: Consumer) {
        print("Consumer is Ready")
        if let highLevelConsumer = consumer as? HighLevelConsumer {
            highLevelConsumer.poll()
        }
    }

    func topicPartitionLeaderNotFound(topic: String, partition: Int32) {
        print("Topic Partition Leader Not Found")
    }
    
    func shouldCommitOffset(topic: String, partition: Int32, offset: Int64) -> Bool {
        print("Should Commit Offset")
        return true
    }

    func shouldAttachOffsetMetadata(topic: String, partition: Int32, offset: Int64) -> String? {
        print("Should Attach Offset Metadata")
        return "mooser"
    }
    
    func offsetDidCommit(topic: String, partition: Int32, offset: Int64) {
        print("Offset Did Commit")
    }

    func offsetCommitDidFail(
        topic: String,
        partition: Int32,
        offset: Int64,
        errorId: Int16,
        errorDescription: String
    ) {
        print("Offset Commit Did Fail")
    }
    
    func shouldRetryFailedOffsetCommit(
        topic: String,
        partition: Int32,
        offset: Int64,
        errorId: Int16,
        errorDescription: String
    ) -> Bool {
        return false
    }

    override func didReceiveMemoryWarning() {
        super.didReceiveMemoryWarning()
        // Dispose of any resources that can be recreated.
    }

}

