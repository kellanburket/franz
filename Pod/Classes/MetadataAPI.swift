//
//  MetadataAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


enum MetadataError: ErrorType {
    case NoSuchTopic(topic: String)
    case NoSuchPartition(partition: Int32)
    case NoSuchBroker(id: Int32)
    case NoLeaderExistsForPartition(partition: Int32)
}


class TopicMetadataRequest: KafkaRequest {

    convenience init(topic: String) {
        self.init(message: TopicMetadataRequestMessage(values: [topic]))
    }

    convenience init(topics: [String] = []) {
        self.init(message: TopicMetadataRequestMessage(values: topics))
    }
    
    init(message: TopicMetadataRequestMessage) {
        super.init(
            apiKey: ApiKey.MetadataRequest,
            value: message
        )
    }

}

class TopicMetadataRequestMessage: KafkaClass {

    var values: KafkaArray<KafkaString>
    
    init(values: [String]) {
        var strings = [KafkaString]()

        for value in values {
            strings.append(KafkaString(value: value))
        }
        
        self.values = KafkaArray(values: strings)
    }

    required init(inout bytes: [UInt8]) {
        values = KafkaArray(bytes: &bytes)
    }

    var length: Int {
        return values.length
    }
    
    var data: NSData {
        return values.data
    }

    var description: String {
        return ""
    }
}

class MetadataResponse: KafkaResponse {
    
    private var _metadataBrokers: KafkaArray<Broker>
    private var _topicMetadata: KafkaArray<Topic>
    
    var brokers: [Int32: Broker] {
        var values = [Int32: Broker]()
        for value in _metadataBrokers.values {
            values[value.nodeId] = value
        }
        return values
    }
    
    var topics: [String: Topic] {
        var values = [String: Topic]()
        for value in _topicMetadata.values {
            values[value.name] = value
        }
        return values
    }
    
    override var description: String {
        var description = "BROKERS:\n"
        for (_, broker) in brokers {
            description += "-----------\n\(broker.description)\n"
        }

        description += "-----------\nMETADATA:\n"
        for (_, topic) in topics {
            description += "-----------\n\(topic.description)\n"
        }
        
        return description
    }
    
    required init(inout bytes: [UInt8]) {
        _metadataBrokers = KafkaArray<Broker>(bytes: &bytes)
        _topicMetadata = KafkaArray<Topic>(bytes: &bytes)
        super.init(bytes: &bytes)
    }
}
