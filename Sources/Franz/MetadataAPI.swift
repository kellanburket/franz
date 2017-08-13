//
//  MetadataAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


class TopicMetadataRequest: KafkaRequest {

    convenience init(topic: String) {
        self.init(message: TopicMetadataRequestMessage(values: [topic]))
    }

    convenience init(topics: [String] = []) {
        self.init(message: TopicMetadataRequestMessage(values: topics))
    }
    
    init(message: TopicMetadataRequestMessage) {
        super.init(
            apiKey: ApiKey.metadataRequest,
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

    required init(bytes: inout [UInt8]) {
        values = KafkaArray(bytes: &bytes)
    }

    lazy var length: Int = {
        return self.values.length
    }()
    
    lazy var data: Data = {
        return (self.values.data)
    }()

    lazy var description: String = {
        return "\tTOPICS(\(self.values.length)):\n\t\t" +
            self.values.description
    }()
}

class MetadataResponse: KafkaResponse {
    
    private var _metadataBrokers: KafkaArray<Broker>
    private var _topicMetadata: KafkaArray<KafkaTopic>
    
    var brokers: [Int32: Broker] {
        var values = [Int32: Broker]()
        for value in _metadataBrokers.values {
            values[value.nodeId] = value
        }
        return values
    }
    
    var topics: [String: KafkaTopic] {
        var values = [String: KafkaTopic]()
        for value in _topicMetadata.values {
            if let name = value.name {
                values[name] = value
            }
        }
        return values
    }
    
    var description: String {
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
    
    required init(bytes: inout [UInt8]) {
        _metadataBrokers = KafkaArray<Broker>(bytes: &bytes)
        _topicMetadata = KafkaArray<KafkaTopic>(bytes: &bytes)
    }
}
